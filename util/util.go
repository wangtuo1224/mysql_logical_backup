package util

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func dirents(path string) ([]os.FileInfo, bool) {
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
		return nil, false
	}
	return entries, true
}

// delete expired backup dirs
func WalkDir(path string, keepDays int) {
	fTime := "-" + strconv.Itoa(keepDays*24) + "h"
	keepTime, _ := time.ParseDuration(fTime)
	log.Printf("before created at->%s will be removed", time.Now().Add(keepTime).Format("2006-01-02 15:04:05"))
	entries, ok := dirents(path)
	if !ok {
		log.Panic("can not find this dir path!!")
	}
	for _, e := range entries {
		if e.ModTime().Before(time.Now().Add(keepTime)) {
			log.Printf("check %s modtime is %s expired,it will be removed!!!", filepath.Join(path, e.Name()), e.ModTime().Format("2006-01-02 15:04:05"))
			if err := os.RemoveAll(filepath.Join(path, e.Name())); err != nil {
				log.Panicf("remove files have errors->%s", err)
			}
		} else {
			log.Printf("check %s modtime is %s not expired,it will be keep!", filepath.Join(path, e.Name()), e.ModTime().Format("2006-01-02 15:04:05"))
		}
	}
}

func FileOrDirIfExists(binfile string) bool {
	_, err := os.Stat(binfile)
	if err != nil {
		return false
	}
	return true
}

func DumpUsers(backupDir string, mysqlUser *string, mysqlPassword *string, mysqlHost *string, mysqlPort *int, ptBin *string, zipBin *string) error {
	ptArgs := []string{}
	// error ptArgs = append(ptArgs,fmt.Sprintf("--user=%s --password=%s --host=%s --port=%d", mysqlUser, mysqlPassword, *mysqlHost, *mysqlPort))
	ptArgs = append(ptArgs, fmt.Sprintf("--user=%s", *mysqlUser))
	ptArgs = append(ptArgs, fmt.Sprintf("--password=%s", *mysqlPassword))
	ptArgs = append(ptArgs, fmt.Sprintf("--host=%s", *mysqlHost))
	ptArgs = append(ptArgs, fmt.Sprintf("--port=%d", *mysqlPort))
	dumpUsersCmd := exec.Command(*ptBin, ptArgs...)
	grant_sql, err := dumpUsersCmd.Output()
	if err != nil {
		log.Panicf("run pt-show-grants have errors->%s", err)
	}

	if err := ioutil.WriteFile(backupDir+"/user.sql", grant_sql, 0666); err != nil {
		log.Panicf("write users to file have errors->%s", err)
	}

	if err := os.Chdir(backupDir); err != nil {
		log.Panicf("change dir have errors->%s", err)
	}

	zip_cmd := exec.Command(*zipBin, "-mq", "user.sql.zip", "user.sql")
	var zip_cmd_out bytes.Buffer
	var zip_cmd_stderr bytes.Buffer
	zip_cmd.Stdout = &zip_cmd_out
	zip_cmd.Stderr = &zip_cmd_stderr
	zipErr := zip_cmd.Run()
	if zipErr != nil {
		log.Panicln(fmt.Sprint(zipErr) + ":" + zip_cmd_stderr.String())
	}
	return nil
}

func DumpSchemas(db *sql.DB, backupDir string, mysqlUser *string, mysqlPassword *string, mysqlHost *string, mysqlPort *int, mysqldumpBin *string, zipBin *string) error {
	dumpSchemasCmd := GetDumpSchemasCmd(db, backupDir, mysqlUser, mysqlPassword, mysqlHost, mysqlPort)
	for _, dumpArgs := range dumpSchemasCmd {
		mysqldump_cmd := exec.Command(*mysqldumpBin, dumpArgs...)
		var mysqldump_cmd_out bytes.Buffer
		var mysqldump_cmd_stderr bytes.Buffer
		mysqldump_cmd.Stdout = &mysqldump_cmd_out
		mysqldump_cmd.Stderr = &mysqldump_cmd_stderr
		mysqldumpErr := mysqldump_cmd.Run()
		if mysqldumpErr != nil {
			log.Panicln(fmt.Sprint(mysqldumpErr) + ":" + mysqldump_cmd_stderr.String())
		}
		//fmt.Println(mysqldump_cmd.Args)

		if err := os.Chdir(backupDir); err != nil {
			log.Panicf("change dir have errors->%s", err)
		}
		zip_cmd := exec.Command(*zipBin, "-mq", dumpArgs[11]+".schemas.sql.zip", dumpArgs[11]+".schemas.sql")
		var zip_cmd_out bytes.Buffer
		var zip_cmd_stderr bytes.Buffer
		zip_cmd.Stdout = &zip_cmd_out
		zip_cmd.Stderr = &zip_cmd_stderr
		zipErr := zip_cmd.Run()
		if zipErr != nil {
			log.Panicln(fmt.Sprint(zipErr) + ":" + zip_cmd_stderr.String())
		}
	}
	return nil
}

func DumpTables(db *sql.DB, backupDir string, mysqlUser *string, mysqlPassword *string, mysqlHost *string, mysqlPort *int, mysqldumpBin *string, zipBin *string) error {
	dumpTablesCmd := GetDumpTablesCmd(db, backupDir, mysqlUser, mysqlPassword, mysqlHost, mysqlPort)
	for _, dumpArgs := range dumpTablesCmd {
		mysqldump_cmd := exec.Command(*mysqldumpBin, dumpArgs...)
		var mysqldump_cmd_out bytes.Buffer
		var mysqldump_cmd_stderr bytes.Buffer
		mysqldump_cmd.Stdout = &mysqldump_cmd_out
		mysqldump_cmd.Stderr = &mysqldump_cmd_stderr
		mysqldumpErr := mysqldump_cmd.Run()
		if mysqldumpErr != nil {
			log.Panicln(fmt.Sprint(mysqldumpErr) + ":" + mysqldump_cmd_stderr.String())
		}

		if err := os.Chdir(backupDir); err != nil {
			log.Panicf("change dir have errors->%s", err)
		}
		zip_cmd := exec.Command(*zipBin, "-mq", dumpArgs[7]+"."+dumpArgs[8]+".sql.zip", dumpArgs[7]+"."+dumpArgs[8]+".sql")
		var zip_cmd_out bytes.Buffer
		var zip_cmd_stderr bytes.Buffer
		zip_cmd.Stdout = &zip_cmd_out
		zip_cmd.Stderr = &zip_cmd_stderr
		zipErr := zip_cmd.Run()
		if zipErr != nil {
			log.Panicln(fmt.Sprint(zipErr) + ":" + zip_cmd_stderr.String())
		}
	}
	return nil
}

func GetDumpTablesCmd(db *sql.DB, backupDir string, mysqlUser *string, mysqlPassword *string, mysqlHost *string, mysqlPort *int) [][]string {
	rows, err := db.Query("select table_schema,table_name from information_schema.tables where table_schema not in ('information_schema','performance_schema','mysql','sys')")
	if err != nil {
		log.Panicf("get tables from mysql have errors->%s", err)
	}

	tableSchema := ""
	tableName := ""

	dumpTablesCmd := [][]string{}
	for rows.Next() {
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			log.Panicf("get tableSchema and tableSchema have errors->%s", err)
		}
		mysqldumpArgs := make([]string, 0, 16)
		mysqldumpArgs = append(mysqldumpArgs, "--user=" + *mysqlUser)
		mysqldumpArgs = append(mysqldumpArgs, "--password=" + *mysqlPassword)
		mysqldumpArgs = append(mysqldumpArgs, "--host=" + *mysqlHost)
		mysqldumpArgs = append(mysqldumpArgs, "--port=" + strconv.Itoa(*mysqlPort))
		mysqldumpArgs = append(mysqldumpArgs, "--single-transaction")
		mysqldumpArgs = append(mysqldumpArgs, "--triggers")
		mysqldumpArgs = append(mysqldumpArgs, "--skip-add-drop-table")
		mysqldumpArgs = append(mysqldumpArgs, tableSchema)
		mysqldumpArgs = append(mysqldumpArgs, tableName)
		mysqldumpArgs = append(mysqldumpArgs, "--result-file")
		mysqldumpArgs = append(mysqldumpArgs, backupDir + "/" + tableSchema + "." + tableName + ".sql")
		dumpTablesCmd = append(dumpTablesCmd, mysqldumpArgs)

	}
	log.Printf("it will dump %d tables", len(dumpTablesCmd))
	return dumpTablesCmd
}

func GetDumpSchemasCmd(db *sql.DB, backupDir string, mysqlUser *string, mysqlPassword *string, mysqlHost *string, mysqlPort *int) [][]string {
	rows, err := db.Query("select distinct(table_schema) from information_schema.tables where table_schema not in ('information_schema','performance_schema','mysql','sys')")
	if err != nil {
		log.Panicf("get table schema from mysql have errors->", err)
	}

	tableSchema := ""
	dumpSchemasCmd := [][]string{}
	for rows.Next() {
		if err := rows.Scan(&tableSchema); err != nil {
			log.Panicf("get tableSchema have errors->%s", err)
		}
		mysqldumpArgs := make([]string, 0, 16)
		mysqldumpArgs = append(mysqldumpArgs, "--user=" + *mysqlUser)
		mysqldumpArgs = append(mysqldumpArgs, "--password=" + *mysqlPassword)
		mysqldumpArgs = append(mysqldumpArgs, "--host=" + *mysqlHost)
		mysqldumpArgs = append(mysqldumpArgs, "--port=" + strconv.Itoa(*mysqlPort))
		mysqldumpArgs = append(mysqldumpArgs, "--single-transaction")
		mysqldumpArgs = append(mysqldumpArgs, "--triggers")
		mysqldumpArgs = append(mysqldumpArgs, "-R")
		mysqldumpArgs = append(mysqldumpArgs, "-E")
		mysqldumpArgs = append(mysqldumpArgs, "-d")
		mysqldumpArgs = append(mysqldumpArgs, "-n")
		mysqldumpArgs = append(mysqldumpArgs, "--skip-add-drop-table")
		mysqldumpArgs = append(mysqldumpArgs, tableSchema)
		mysqldumpArgs = append(mysqldumpArgs, "--result-file")
		mysqldumpArgs = append(mysqldumpArgs, backupDir + "/" + tableSchema + ".schemas.sql")
		dumpSchemasCmd = append(dumpSchemasCmd, mysqldumpArgs)
	}

	return dumpSchemasCmd
}

type DiskStatus struct {
	All  uint64 `json:"all"`
	Used uint64 `json:"used"`
	Free uint64 `json:"free"`
}

// disk usage of path/disk
func DiskUsage(path string) (disk DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}
