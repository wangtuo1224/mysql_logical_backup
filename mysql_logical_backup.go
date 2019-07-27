package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"log"
	"mysql_logical_backup/util"
	"os"
	"time"
)

var (
	backupPath = kingpin.Flag(
		"backup.path",
		"Path Under Which To Store Backup Files.",
	).Default("/tmp").String()
	backupFlag = kingpin.Flag(
		"backup.flag",
		"Backup Flag.").Default("AWS").String()
	mysqldumpBin = kingpin.Flag(
		"mysqldumpbin.dir",
		"Where is mysqldump.",
	).Default("/usr/bin/mysqldump").String()
	ptBin = kingpin.Flag(
		"ptbin.dir",
		"Where is pt-show-grants.",
	).Default("/bin/pt-show-grants").String()
	zipBin = kingpin.Flag(
		"zip.dir",
		"Where is zip.",
	).Default("/usr/bin/zip").String()
	keepDays      = kingpin.Flag("mysql.keepdays", "MySQL Backup Keep Days.").Default("3").Int()
	mysqlHost     = kingpin.Flag("mysql.host", "MySQL Server Host.").Default("127.0.0.1").String()
	mysqlPort     = kingpin.Flag("mysql.port", "MySQL Server Port.").Default("6033").Int()
	mysqlUser     = kingpin.Flag("mysql.user", "MySQL Server User.").Default("backup_user").String()
	mysqlPassword = kingpin.Flag("mysql.password", "MySQL Server Password.").Default("").String()
	emailRecivers = kingpin.Flag("email.recivers", "Send email to users.").Default("").String()
	emailServerHost = kingpin.Flag("email.serverHost", "Email server host.").Default("").String()
	emailServerPort = kingpin.Flag("email.serverPort", "Email server port.").Default("0").Int()
	fromEmail = kingpin.Flag("email.from", "Email from user.").Default("").String()
	fromPassword = kingpin.Flag("email.password", "Email from user's password.").Default("").String()
	hourMinSec    = time.Now().Format("20060102150405")
)

func main() {
	kingpin.HelpFlag.Short('h')
	kingpin.Version("0.1")
	kingpin.Parse()
	logFileName := "/tmp/mysqldump_" + *backupFlag + ".log"
	basicDir := *backupPath + "/" + *backupFlag
	backupDir := *backupPath + "/" + *backupFlag + "/" + hourMinSec

	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666) //打开日志文件，不存在则创建
	if err != nil {
		log.Panicf("open file %s have errors!!!", logFileName)
	}
	defer logFile.Close()

	log.SetOutput(logFile)              //设置输出流
	log.SetPrefix("[mysqldump]")        //日志前缀
	log.SetFlags(log.Ldate | log.Ltime) //日志输出样式

	if !util.FileOrDirIfExists(basicDir) {
		err := os.MkdirAll(basicDir, os.ModePerm)
		if err != nil {
			log.Panicf("create basic dir %s have errors!!!", basicDir)
		}
		log.Printf("create basic dir %s success!", basicDir)
	}

	log.Println("check expired backup...")
	util.WalkDir(basicDir, *keepDays)

	log.Println("check mysqldump if exists...")
	if !util.FileOrDirIfExists(*mysqldumpBin) {
		log.Panicf("can not find mysqldump on %s!", *mysqldumpBin)
	}
	log.Printf("find mysqldump location is %s", *mysqldumpBin)

	log.Println("check pt-show-grants if exists...")
	if !util.FileOrDirIfExists(*ptBin) {
		log.Panicf("can not find pt-show-grants on %s!", *ptBin)
	}
	log.Printf("find pt-show-grants location is %s", *ptBin)

	log.Println("check zip if exists...")
	if !util.FileOrDirIfExists(*zipBin) {
		log.Panicf("can not find zip on %s!", *zipBin)
	}
	log.Printf("find zip location is %s", *zipBin)

	if err := os.MkdirAll(backupDir, os.ModePerm); err != nil {
		log.Panicf("create backup directory have errors->%s", err)
	}
	log.Printf("create backup directory %s success!", backupDir)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", *mysqlUser, *mysqlPassword, *mysqlHost, *mysqlPort)
	db, _ := sql.Open("mysql", dsn)
	if err := db.Ping(); err != nil {
		log.Panicf("access mysql have errors->%s", err)
	}
	defer db.Close()

	log.Printf("mysqldump start at->%s", time.Now().Format("2006-01-02 15:04:05"))
	stime := time.Now()
	// 初始化发邮件相关参数, 覆盖默认参数
	u := util.InitNewUser()
	if u.ServerHost == "" {
		u.ServerHost = *emailServerHost
	}
	if u.ServerPort == 0 {
		u.ServerPort = *emailServerPort
	}
	if u.FromEmail == "" {
		u.FromEmail = *fromEmail
	}
	if u.FromPassword == "" {
		u.FromPassword = *fromPassword
	}
	if u.Toers == "" {
		u.Toers = *emailRecivers
	}

	util.InitEmail(u)

	// dump users
	if err := util.DumpUsers(backupDir, mysqlUser, mysqlPassword, mysqlHost, mysqlPort, ptBin, zipBin); err != nil {
		log.Panicf("run pt-show-grants have errors->%s", err)
	}
	log.Println("dump users success!")

	// dump schemas include triggers events routines table structure
	if err := util.DumpSchemas(db, backupDir, mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqldumpBin, zipBin); err != nil {
		log.Panicf("dump schemas have errors->%s", err)
	}
	log.Println("dump schemas success!")

	// dump tables and data
	if err := util.DumpTables(db, backupDir, mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqldumpBin, zipBin); err != nil {
		//util.SendEmail("dump tables and data have errors", err.Error(), logFileName)
		log.Panicf("dump tables and data have errors->%s", err)
	}
	log.Println("dump tables and data success!")

	etime := time.Now()
	log.Printf("finish at->%s", etime.Format("2006-01-02 15:04:05"))
	log.Printf("consuming time->%s", etime.Sub(stime).Round(time.Second))

	diskUsage := util.DiskUsage(basicDir)
	log.Printf("backup directory %s disk used->%dG,disk free->%dG", basicDir, diskUsage.Used/1024/1024/1024, diskUsage.Free/1024/1024/1024)

	data, err := ioutil.ReadFile(logFileName)
	if err != nil {
		log.Panicf("file reading error->%s", err)
	}

	err = util.SendEmail("mysqldump "+*backupFlag+" logs", string(data), logFileName)
	if err != nil {
		log.Panicf("sendmail error->%s", err)
	}
	log.Printf("sendmail success!")
}