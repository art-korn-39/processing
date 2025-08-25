package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/tealeg/xlsx"
)

func Round(x float64, decimals int) float64 {

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
	m2 := math.Pow(10, float64(n)-float64(decimals))
	x3 := x2 / m2
	x4 := math.Round(x3)
	m3 := math.Pow(10, float64(decimals))
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

func Equals(numA, numB float64) bool {
	const TOLERANCE = 1e-8
	delta := math.Abs(numA - numB)
	return delta < TOLERANCE
}

func FormatInt[v int | int64](n v) string {
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

func FloatFromCell(cell *xlsx.Cell) (num float64) {

	var err error
	num, err = cell.Float()
	if err != nil {
		str := cell.String()
		str = strings.TrimSpace(str)
		if str == "" {
			return 0
		} else {
			num, _ = strconv.ParseFloat(str, 64)
		}
	}

	if math.IsNaN(num) {
		num = 0
	}

	return

}

func ParseFloat(str string) (float64, error) {
	if strings.Contains(str, ".") {
		str = strings.ReplaceAll(str, ",", "")
	} else {
		str = strings.ReplaceAll(str, ",", ".")
	}
	return strconv.ParseFloat(str, 64)
}

func FloatToString(f float64, accuracy int) string {
	mask := fmt.Sprint("%.", accuracy, "f")
	return strings.ReplaceAll(fmt.Sprintf(mask, f), ".", ",")
}

func AddCellWithFloat(row *xlsx.Row, f float64, accuracy int) {

	cell := row.AddCell() //26
	cell.SetFloat(f)

	s := make([]string, 0, 8)

	for i := 0; i < accuracy; i++ {
		s = append(s, "0")
	}

	format := fmt.Sprint("0.", strings.Join(s, ""))

	cell.SetFormat(format)

}
