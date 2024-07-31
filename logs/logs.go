package logs

import (
	"app/config"
	"fmt"
	"log"
	"os"
	"time"
)

type LogType int

// Regl - регламентно из 1С (важное + время)
// Debug - мануально из IDE
// Testing - тестирование из IDE
// Остальное - пользователями мануально из 1С (без времени)

const (
	INFO LogType = iota // (Regl = 0) value
	ERROR
	FATAL // value
	DEBUG // (Debug = 1) c.value
	REGL  // (Regl = 1) f.value | (Regl = 0) c.value
	MAIN
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

	// для пользователя интерактивно
	case INFO:
		if !config.Get().Routine_task {
			file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			defer file.Close()

			file.WriteString(fmt.Sprintf("%s\n", value))
			log.Println(value)
		}

	// всегда
	case MAIN:
		file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		defer file.Close()

		if config.Get().Routine_task {
			file.WriteString(fmt.Sprintf("[%s] %s\n", time.Now().Format(time.DateTime), value))
		} else {
			file.WriteString(fmt.Sprintf("%s\n", value))
		}

		log.Println(value)

	case ERROR:
		file, _ := os.OpenFile(config.Get().File_errors, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		defer file.Close()

		file.WriteString(fmt.Sprintf("[%s] %s\n", time.Now().Format(time.DateTime), value))

		if config.Debug {
			log.Println(value)
		}

	case FATAL:
		file, _ := os.OpenFile(config.Get().File_logs, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		defer file.Close()

		file.WriteString(fmt.Sprintf("%s\n", value))
		log.Fatal(value)

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

	}

}

func Finish() {
	r := recover()
	if r != nil {
		Add(INFO, r)
	}
}
