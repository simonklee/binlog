#!/bin/bash

mkdir -p lib
echo "Name: GoMysql" > lib/gomysql.pc
echo "Description: Flags for using mysql C client in go" >> lib/gomysql.pc
echo "Version:" "$(mysql_config --version)" >> lib/gomysql.pc 
echo "Cflags:" "$(mysql_config --cflags) -ggdb -fPIC" >> lib/gomysql.pc
echo "Libs:" "$(mysql_config --libs_r | sed 's,-lmysqlclient_r,-l:libmysqlclient.a -lstdc++,')" >> lib/gomysql.pc
export PKG_CONFIG_PATH=lib
