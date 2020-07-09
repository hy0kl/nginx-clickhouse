package process

import (
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"

	"github.com/astaxie/beego/config"
	"github.com/astaxie/beego/logs"
	"github.com/jmoiron/sqlx"
)

var (
	cfg config.Configer
	db  *sqlx.DB
)

func init() {
	var err error
	cfg, err = config.NewConfig("ini", "conf/app.conf")
	if err != nil {
		panic(fmt.Sprintf("can not parse config file: conf/app.conf, %v\n", err))
	}

	driverName := cfg.String("driver_name")
	logs.Debug("driver_name: %s", driverName)
	dataSourceName := cfg.String("data_source_name")

	db, err = sqlx.Open(driverName, dataSourceName)
	if err != nil {
		panic(fmt.Sprintf("can not connect db, source: %s, err: %v", dataSourceName, err))
	}

	err = db.Ping()
	if err != nil {
		panic(fmt.Sprintf("ping db exception, %v", err))
	}

	logs.Debug("db init success.")
}

func Work() {

}
