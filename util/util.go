package util

import (
	"app/logs"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
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

// С 0 (вкл.) до last (искл.)
func SubString(s string, first int, last int) string {

	runes := []rune(s)
	length := len(runes)

	if length <= last {
		last = length
	}

	if last == 0 {
		last = length
	}

	return string(runes[first:last])

}

func Round(x float64, decimals float64) float64 {

	//0. 0.64496
	//1. узнать количество знаков после запятой (n) [5]
	//2. умножить на 10^n (m1), чтобы остались только целые [64496]
	//3. округлить [64496]
	//4. поделить на 10^(n-dcm) [64.496]
	//5. округлить [64]
	//6. поделить на 10^dcm

	//test cases: 0.235, 0.64496, 0.16499999

	if math.IsNaN(x) {
		return x
	}

	if x == 0.0 {
		return x
	}

	x = BaseRound(x)

	str := strconv.FormatFloat(x, 'f', -1, 64)
	i := strings.Index(str, ".")
	n := len(SubString(str, i+1, 0))

	m1 := math.Pow(10, float64(n))
	x1 := x * m1
	x2 := math.Round(x1)
	m2 := math.Pow(10, float64(n)-decimals)
	x3 := x2 / m2
	x4 := math.Round(x3)
	m3 := math.Pow(10, decimals)
	x5 := x4 / m3

	return x5

}

// очистка от 9999 или 00001 после запятой
func BaseRound(x float64) float64 {

	m := 10000000000.0
	x1 := x * m
	x2 := math.Round(x1)
	x3 := x2 / m
	return x3

}

// format: "2024-05-22T12:45:41+0300"
func GetDateFromString(s string) time.Time {

	index := strings.Index(s, "+")
	if index != -1 {
		s = SubString(s, 0, index)
	}
	s = strings.ReplaceAll(s, "T", " ")

	v, _ := time.Parse(time.DateTime, s)

	return v

}

func TruncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
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

func FormatInt(n int) string {
	in := strconv.FormatInt(int64(n), 10)
	numOfDigits := len(in)
	if n < 0 {
		numOfDigits-- // First character is the - sign (not a digit)
	}
	numOfCommas := (numOfDigits - 1) / 3

	out := make([]byte, len(in)+numOfCommas)
	if n < 0 {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ' '
		}
	}
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
