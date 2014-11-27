package binlog

import (
	"testing"

	"github.com/simonz05/binlog/mysql"
)

func TestConnection(t *testing.T) {
	params := mysql.ConnectionParams{
		Host:       "127.0.0.1",
		Port:       3306,
		Uname:      "root",
		Pass:       "",
		DbName:     "testing",
		UnixSocket: "",
		Charset:    "utf8",
		Flags:      0,
	}

	conn, err := NewConnection(params)
	if err != nil {
		t.Fatalf("err opening connection %v", err)
	}

	err = conn.StartBinlogDump(245)

	if err != nil {
		t.Fatalf("err start binlog %v", err)
	}
}
