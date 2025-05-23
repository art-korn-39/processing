package aws

import (
	"app/config"
	"app/file"
	"app/logs"
	"app/querrys"
	"app/storage"
	"app/util"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

// bo_report_2024-03-13T03:00:00+00:00_2024-03-14T03:00:00+00:00.csv (до 22.01.2025)
// bo_report_2025-01-23T21:00:00+00:00_2025-01-24T21:00:00+00:00.csv
// bo_report_2025-01-24T00:00:00+03:00_2025-01-25T00:00:00+03:00.csv
const REG_EXP = `^bo_report_\d{4}-\d{2}-\d{2}T(03|21|00):00:00\+(00|03):00_\d{4}-\d{2}-\d{2}T(03|21|00):00:00\+(00|03):00.csv`

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}
	defer storage.Close()

	files, err := getFiles(cfg, storage)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	loadIntoClickhouse(cfg, storage, files)

}

func getFiles(cfg config.Config, storage *storage.Storage) ([]*file.FileInfo, error) {

	svc := s3.New(storage.AWS)
	input := &s3.ListObjectsInput{
		Bucket: &cfg.AWS.Bucket,
	}

	result, err := svc.ListObjects(input)
	if err != nil {
		return nil, err
	}

	files := make([]*file.FileInfo, 0, len(result.Contents))

	r, _ := regexp.Compile(REG_EXP)

	for _, v := range result.Contents {
		name := *v.Key
		bytes := *v.Size
		modified := *v.LastModified

		match := r.MatchString(name)

		if match {

			file := &file.FileInfo{
				Filename: name,
				Category: file.REG_BOF,
				Size:     int(bytes),
				Size_mb:  int(bytes) / 1024000,
				Modified: modified,
			}

			files = append(files, file)

		}
	}

	return files, nil

}

func loadIntoClickhouse(cfg config.Config, storage *storage.Storage, files []*file.FileInfo) (err error) {

	var insert_done bool
	var sum_rows_added, rows_added, rows_before, rows_after int64

	for _, f := range files {

		f.GetLastUpload(storage.Postgres)
		if f.LastUpload.After(f.Modified) {
			continue
		}

		err := storage.Clickhouse.Get(&rows_before, "SELECT count(*) FROM reports")
		if err != nil {
			logs.Add(logs.ERROR, err)
		}

		var stat string
		if fileBefore250624(f) {
			stat = querrys.Stat_Insert_reports_before_250624()
		} else {
			stat = querrys.Stat_Insert_reports()
		}

		stat = strings.ReplaceAll(stat, "$region", cfg.AWS.Region)
		stat = strings.ReplaceAll(stat, "$bucket", cfg.AWS.Bucket)
		stat = strings.ReplaceAll(stat, "$filename", f.Filename)
		stat = strings.ReplaceAll(stat, "$key", cfg.AWS.Key)
		stat = strings.ReplaceAll(stat, "$secret", cfg.AWS.Secret)

		_, err = storage.Clickhouse.Exec(stat)
		if err != nil {
			logs.Add(logs.ERROR, "[Insert] Clickhouse.Exec() file:", f.Filename, "\n", err)
			//return err
		} else {

			insert_done = true

			err := storage.Clickhouse.Get(&rows_after, "SELECT count(*) FROM reports")
			if err != nil {
				logs.Add(logs.ERROR, err)
			}

			rows_added = rows_after - rows_before
			sum_rows_added += rows_added

			f.Rows = int(rows_added)
			f.LastUpload = time.Now()
			f.InsertIntoDB(storage.Postgres, 0)
		}

		//break
	}

	if insert_done {
		stat := querrys.Stat_Optimize_reports()
		_, err := storage.Clickhouse.Exec(stat)
		if err != nil {
			logs.Add(logs.ERROR, "[Optimize] Clickhouse.Exec()", err)
			//return err
		}
	}

	logs.Add(logs.MAIN, "Строк добавлено: ", sum_rows_added)

	return nil
}

func fileBefore250624(file *file.FileInfo) bool {

	border := time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC)

	date_str := util.SubString(file.Filename, 10, 20)

	date, _ := time.Parse(time.DateOnly, date_str)

	return date.Before(border)

}
