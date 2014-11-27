package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/simonz05/binlog/mysql"
	"github.com/simonz05/util/log"
)

// Connection represents a connection to mysql that pretends to be a slave
// connecting for replication. Each such connection must identify itself to
// mysqld with a server ID that is unique both among other Connections and
// among actual slaves in the topology.
type Connection struct {
	*mysql.Connection
	slaveID uint32
}

// NewConnection creates a new slave connection to the mysql instance.
func NewConnection(params mysql.ConnectionParams) (*Connection, error) {
	conn, err := mysql.Connect(params)

	if err != nil {
		return nil, err
	}

	return &Connection{
		Connection: conn,
		slaveID:    30, // TODO: fix
	}, nil
}

func (c *Connection) StartBinlogDump(pos uint32) error {
	err := c.sendBinlogDumpCommand(pos)

	if err != nil {
		return err
	}

	buf, err := c.ReadPacket()

	if err != nil {
		log.Errorf("couldn't start binlog dump: %v", err)
		return err
	}

	for {
		if buf[0] == 254 {
			// The master is telling us to stop.
			log.Printf("received EOF packet in binlog dump: %#v", buf)
			return nil
		}

		fmt.Println(buf[1:])
		//select {
		//// Skip the first byte because it's only used for signaling EOF.
		//case eventChan <- flavor.MakeBinlogEvent(buf[1:]):
		//case <-svc.ShuttingDown:
		//	return nil
		//}

		buf, err = c.ReadPacket()

		if err != nil {
			if sqlErr, ok := err.(*mysql.SqlError); ok && sqlErr.Number() == 2013 {
				// errno 2013 = Lost connection to MySQL server during query
				// This is not necessarily an error. It could just be that we closed
				// the connection from outside.
				log.Printf("connection closed during binlog stream (possibly intentional): %v", err)
				return err
			}
			log.Errorf("read error while streaming binlog events: %v", err)
			return err
		}
	}
	return nil
}

func (c *Connection) sendBinlogDumpCommand(pos uint32) error {
	const COM_BINLOG_DUMP = 0x12

	// Tell the server that we understand GTIDs by setting our slave capability
	// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := c.ExecuteFetch("SET @mariadb_slave_capability=4", 0, false); err != nil {
		return fmt.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Tell the server that we understand the format of events that will be used
	// if binlog_checksum is enabled on the server.
	if _, err := c.ExecuteFetch("SET @master_binlog_checksum=@@global.binlog_checksum", 0, false); err != nil {
		return fmt.Errorf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP to
	// provide the start position in GTID form.
	query := fmt.Sprintf("SET @slave_connect_state='1-1-%d'", pos)
	if _, err := c.ExecuteFetch(query, 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_connect_state='1-1-%d': %v", pos, err)
	}

	// Real slaves set this upon connecting if their gtid_strict_mode option was
	// enabled. We always use gtid_strict_mode because we need it to make our
	// internal GTID comparisons safe.
	if _, err := c.ExecuteFetch("SET @slave_gtid_strict_mode=1", 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_gtid_strict_mode=1: %v", err)
	}

	// Since we use @slave_connect_state, the file and position here are ignored.
	buf := makeBinlogDumpCommand(pos, 0, c.slaveID, "")
	return c.SendCommand(COM_BINLOG_DUMP, buf)
}

// Close closes the slave connection, which also signals an ongoing dump
// started with StartBinlogDump() to stop and close its BinlogEvent channel.
// The ID for the slave connection is recycled back into the pool.
func (c *Connection) Close() {
	if c.Connection != nil {
		fmt.Printf("force-closing slave socket to unblock reads")
		c.Connection.ForceClose()
		fmt.Printf("closing slave MySQL client, recycling slaveID %v", c.slaveID)
		c.Connection.Close()
		c.Connection = nil
	}
}

// makeBinlogDumpCommand builds a buffer containing the data for a MySQL
// COM_BINLOG_DUMP command.
func makeBinlogDumpCommand(pos uint32, flags uint16, server_id uint32, filename string) []byte {
	var buf bytes.Buffer
	buf.Grow(4 + 2 + 4 + len(filename))

	// binlog_pos (4 bytes)
	binary.Write(&buf, binary.LittleEndian, pos)
	// binlog_flags (2 bytes)
	binary.Write(&buf, binary.LittleEndian, flags)
	// server_id of slave (4 bytes)
	binary.Write(&buf, binary.LittleEndian, server_id)
	// binlog_filename (string with no terminator and no length)
	buf.WriteString(filename)

	return buf.Bytes()
}
