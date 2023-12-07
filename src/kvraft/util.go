package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
//const Debug = 0
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//	return
//}

type logTopic string

const (
	dClient   logTopic = "CLIENT"
	dServer   logTopic = "SERVER"
	dCommand  logTopic = "COMAND"
	dGet      logTopic = "GETOPT"
	dPut      logTopic = "PUTOPT"
	dAppend   logTopic = "APPEND"
	dApply    logTopic = "APPLYS"
	dError    logTopic = "ERRORS"
	dSnapshot logTopic = "SPSHOT"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerboisty()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	//如果VERBOSE = 2, 将日志打印到文件里
	if debugVerbosity == 2 {
		file := "logs.txt"
		logFile, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0766)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
	}
}

// 从环境变量里读到日志level
func getVerboisty() int {
	v := os.Getenv("VERBOSE_KVRAFT")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalln("Invalid verbosity", v)
		}
	}
	return level
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
