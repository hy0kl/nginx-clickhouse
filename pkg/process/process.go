package process

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"

	"github.com/astaxie/beego/config"
	"github.com/astaxie/beego/logs"
	"github.com/hpcloud/tail"
	"github.com/jmoiron/sqlx"
)

const (
	LogFormatJson = `json`
	LogFormatRaw  = `raw`
)

type workEnvT struct {
	DbTableName   string
	LogFormatType string
	LogFieldConf  []string
	LogFiles      []string
}

var (
	cfg     config.Configer
	db      *sqlx.DB
	workEnv workEnvT
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

	workEnv.DbTableName = cfg.String(`db_table_name`)
	if workEnv.DbTableName == "" {
		panic(`need set db_table_name`)
	}

	workEnv.LogFormatType = cfg.String(`log_format_type`)
	if workEnv.LogFormatType != LogFormatJson && workEnv.LogFormatType != LogFormatRaw {
		panic(fmt.Sprintf("log format type out of range, type: %s\n", workEnv.LogFormatType))
	}

	fieldConf := cfg.String(`log_field_conf`)
	err = json.Unmarshal([]byte(fieldConf), &workEnv.LogFieldConf)
	if err != nil {
		panic(fmt.Sprintf("can not json decode for log_filed_conf, json: %s, err: %v\n", fieldConf, err))
	}

	if workEnv.LogFormatType == LogFormatRaw {
		if len(workEnv.LogFieldConf) <= 0 {
			panic(fmt.Sprintf("when format is type, need set 'log_filed_conf' is json array.\n"))
		}
	}

	logsPath := cfg.String(`logs_path`)
	logsNameJson := cfg.String(`logs_name`)
	var fileBox []string
	err = json.Unmarshal([]byte(logsNameJson), &fileBox)
	if err != nil {
		panic(fmt.Sprintf("can not json decode for logs_name, json: %s, err: %v\n", fieldConf, err))
	}
	for _, fileName := range fileBox {
		workEnv.LogFiles = append(workEnv.LogFiles, fmt.Sprintf(`%s/%s`, logsPath, fileName))
	}
	if len(workEnv.LogFiles) <= 0 {
		panic(`need set log files.`)
	}

	logs.Info("workEnv: %#v", workEnv)
}

func Work() {
	for {
		var wg sync.WaitGroup

		// 监听文件变化,并创建es索引数据
		for _, logFilename := range workEnv.LogFiles {
			wg.Add(1)
			go tailFile4DB(&wg, logFilename)
		}

		// 主 goroutine,等待工作 goroutine 正常结束
		wg.Wait()

		// 休眠一下下,给其他进程切换日志的时间
		time.Sleep(5 * time.Minute)
	}
}

func tailFile4DB(wg *sync.WaitGroup, logFilename string) {
	defer wg.Done()

	logs.Informational("[tailFile4DB] tail -f %s", logFilename)

	// 如果是5:00以后重启程序,那么跳过之前的日志,避免重启导入旧日志,但有可能丢失部分日志,暂时不考虑了... {
	tfConf := tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: true,
	}
	topT := time.Now()
	topHour := topT.Hour()
	if topHour >= 5 {
		fileInfo, err := os.Stat(logFilename)
		if err != nil {
			logs.Error("[tailFile4DB] stat file has wrong, logFilename: %s, err: %v", logFilename, err)
			return
		}

		tfConf.Location = &tail.SeekInfo{
			Offset: fileInfo.Size(),
			Whence: io.SeekStart,
		}
		// 下面的方法也有效,并且节省一次文件操作开销
		// &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}
	}
	// end }

	tf, err := tail.TailFile(logFilename, tfConf)
	if err != nil {
		logs.Error("[tailFile4DB] tail -f has wrong, err:", err)
		return
	}

	for line := range tf.Lines {
		var data = make(map[string]interface{})

		var fieldBox []string
		var placeholder []string
		var sqlArgs []interface{}

		if workEnv.LogFormatType == LogFormatJson {
			errD := json.Unmarshal([]byte(line.Text), &data)
			if errD != nil {
				logs.Error("[tailFile4DB] json decode exception, json: %s, err: %v", line.Text, errD)
				continue
			}

			for field, value := range data {
				fieldBox = append(fieldBox, field)
				placeholder = append(placeholder, "?")
				sqlArgs = append(sqlArgs, value)
			}

		} else {
			dataBox := strings.Split(line.Text, "\t")
			if len(workEnv.LogFieldConf) == 0 || len(dataBox) != len(workEnv.LogFieldConf) {
				logs.Error("[tailFile4DB] field mismatch, conf: %v, log: %s", workEnv.LogFieldConf, line.Text)
				continue
			}

			for i, value := range dataBox {
				fieldBox = append(fieldBox, workEnv.LogFieldConf[i])
				placeholder = append(placeholder, "?")
				sqlArgs = append(sqlArgs, value)
			}
		}

		sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, workEnv.DbTableName, strings.Join(fieldBox, ", "), strings.Join(placeholder, ", "))
		_, errI := db.Exec(sql, sqlArgs...)
		if errI != nil {
			logs.Error("[tailFile4DB] db insert get exception, sql: %s, args: %v, err: %v", sql, sqlArgs, errI)
		}

		logs.Debug("[tailFile4DB] origin log: %v", line.Text)
	}

	return
}
