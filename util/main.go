package util

import (
	"app/logs"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"
)

func Unused(...any) {}

// ternary operator
func TR(statement bool, a any, b any) any {
	if statement {
		return a
	}
	return b
}

// take first result
func FR(v any, err error) any {
	// if err != nil {
	// 	log.Println("error encountered when none assumed:", err)
	// }
	return v
}

// "ptr_struct_in" указатель на структуру приемник, "struct_src" структура источник
func FillValuesOfStruct(ptr_struct_in, struct_src any) {

	ptr_value_in := reflect.ValueOf(ptr_struct_in)
	value_src := reflect.ValueOf(struct_src)

	if ptr_value_in.Kind() != reflect.Pointer ||
		value_src.Kind() != reflect.Struct {
		return
	}

	// Получение структуры
	value_in := ptr_value_in.Elem()
	if value_in.Kind() != reflect.Struct {
		return
	}

	numFields_in := value_in.NumField()
	numFields_src := value_src.NumField()

	for i := 0; i < numFields_in; i++ {

		field_in := value_in.Field(i)

		// Проверка на доступность поля для записи, должно быть экспортируемым
		if !field_in.IsValid() || !field_in.CanSet() {
			continue
		}

		type_in := field_in.Type().Name()
		name_in := strings.ToLower(value_in.Type().Field(i).Name)

		for j := 0; j < numFields_src; j++ {

			field_src := value_src.Field(j)
			type_src := field_src.Type().Name()
			name_src := strings.ToLower(value_src.Type().Field(j).Name)

			if name_in == name_src && type_in == type_src {
				field_in.Set(field_src)
				break
			}

		}

	}

}

func ParseFoldersRecursively(folder string) ([]string, error) {

	s := []string{}

	files, err := os.ReadDir(folder)
	if err != nil {
		logs.Add(logs.ERROR, fmt.Sprint("os.ReadDir() ", err))
		return nil, err
	}

	for _, fileDir := range files {

		filename := fmt.Sprint(folder, "\\", fileDir.Name())

		if fileDir.IsDir() {
			s2, err := ParseFoldersRecursively(filename)
			if err != nil {
				logs.Add(logs.ERROR, fmt.Sprint("os.ReadDir() ", err))
			} else {
				s = append(s, s2...)
			}
			continue
		}

		s = append(s, filename)

	}

	return s, nil

}

func ReadJsonFile(data any, filename string) error {

	file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, data)
	if err != nil {
		return err
	}

	return nil

}

type Period struct {
	StartDay time.Time
	EndDay   time.Time
}

func GetChannelOfDays(startDate, finishDate time.Time, duration time.Duration) chan Period {

	channel := make(chan Period, 50)

	go func() {
		startDay := startDate
		for {
			if startDay.After(finishDate) {
				break
			}

			endDay := startDay.Add(duration).Add(-1 * time.Second)
			if endDay.After(finishDate) {
				endDay = finishDate
			}

			period := Period{
				StartDay: startDay,
				EndDay:   endDay,
			}
			channel <- period

			startDay = startDay.Add(duration)
		}
		close(channel)
	}()

	return channel

}
