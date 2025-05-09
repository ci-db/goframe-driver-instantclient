// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package instantclient

import (
	"database/sql"
	"strings"
	"time"

	"github.com/godror/godror"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
)

// Open creates and returns an underlying sql.DB object for oracle with instantclient.
func (d *Driver) Open(config *gdb.ConfigNode) (db *sql.DB, err error) {

	var underlyingDriverName = "instantclient"

	options := map[string]string{
		"CONNECTION TIMEOUT": "60",
		"PREFETCH_ROWS":      "25",
	}

	if config.Debug {
		options["TRACE FILE"] = "oracle_trace.log"
	}
	if config.Extra != "" {
		list := strings.Split(config.Extra, "&")
		for _, v := range list {
			kv := strings.Split(v, "=")
			if len(kv) == 2 {
				options[kv[0]] = kv[1]
			}
		}
	}

	var conn godror.ConnectionParams
	var timeZone *time.Location

	if config.Timezone != "" {
		timeZone, err = time.LoadLocation(config.Timezone)
		if err != nil {
			timeZone = time.Local
		}
	} else {
		timeZone = time.Local
	}

	conn.ConnectString = config.Protocol + "://" + config.Host + ":" + config.Port + "/" + config.Name
	if config.Extra != "" {
		conn.ConnectString = conn.ConnectString + "?" + config.Extra
	}
	conn.Timezone = timeZone
	conn.Username = config.User
	conn.Password = godror.NewPassword(config.Pass)
	conn.MaxSessions = config.MaxOpenConnCount
	conn.MinSessions = config.MaxIdleConnCount
	conn.SessionIncrement = 1
	conn.ConnClass = "POOLED"
	conn.Heterogeneous = sql.NullBool{Bool: false}
	conn.StandaloneConnection = sql.NullBool{Bool: false}
	conn.EnableEvents = false
	conn.IsPrelim = false

	db = sql.OpenDB(godror.NewConnector(conn))
	err = db.Ping()
	if err != nil {
		err = gerror.WrapCodef(
			gcode.CodeDbOperationError, err,
			`sql.Open failed for driver "%s" by connect string "%s"`, underlyingDriverName, conn.ConnectString,
		)
		return nil, err
	}
	return
}
