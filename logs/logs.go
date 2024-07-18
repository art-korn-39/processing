package logs

import (
	"app/config"
	"fmt"
	"log"
	"os"
	"time"
)

type LogType int

const (
	INFO LogType = iota
	ERROR
	FATAL
	DEBUG
	REGL
)

var Testing bool

func Add(t LogType, v ...any) {

	value := fmt.Sprint(v...)

	if Testing {
		if t == FATAL {
			log.Fatal(value)
		}
		return
	}

	switch t {
	case INFO:
		if !config.Get().Routine_task {
			file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			defer file.Close()

			file.WriteString(fmt.Sprintf("%s\n", value))
			log.Println(value)
		}

	case REGL:
		if config.Get().Routine_task {
			file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			defer file.Close()

			file.WriteString(fmt.Sprintf("[%s] %s\n", time.Now().Format(time.DateTime), value))
		}

		log.Println(value)

	case DEBUG:
		if config.Debug {
			fmt.Println(value)
		}

	case ERROR:
		file, _ := os.OpenFile(config.Get().File_errors, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		defer file.Close()

		file.WriteString(fmt.Sprintf("%s\n", value))

		if config.Debug {
			log.Println(value)
		}

	case FATAL:
		file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		defer file.Close()

		file.WriteString(fmt.Sprintf("%s\n", value))
		log.Fatal(value)
	}

}

func Finish() {
	r := recover()
	if r != nil {
		Add(INFO, r)
	}
}
