package conversion

import (
	"app/logs"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileInfo struct {
	Filename   string    `db:"filename"`
	Size       int       `db:"size"`
	Size_mb    int       `db:"size_mb"`
	Modified   time.Time `db:"modified"`
	Rows       int       `db:"rows"`
	LastUpload time.Time `db:"last_upload"`
	Done       bool
}

func GetFiles(filenames []string) []*FileInfo {

	files := make([]*FileInfo, 0, len(filenames))

	for _, f := range filenames {

		if strings.Contains(f, "~$") || filepath.Ext(f) != ".xlsx" {
			continue
		}

		file, err := os.OpenFile(f, os.O_RDONLY, os.FileMode(0400))
		if err != nil {
			err = fmt.Errorf("os.OpenFile() %v", err)
			logs.Add(logs.ERROR, err)
			continue
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			err = fmt.Errorf("file.Stat() %v", err)
			logs.Add(logs.ERROR, err)
			continue
		}

		fileInfo := &FileInfo{
			Filename: f,
			Size:     int(stat.Size()),
			Size_mb:  int(stat.Size()) / 1024000,
			Modified: stat.ModTime(),
		}

		files = append(files, fileInfo)

	}

	return files

}

func (f *FileInfo) InsertIntoDB() {

	if db == nil {
		logs.Add(logs.FATAL, "no connection to postgres")
		return
	}

	if f.Done {
		return
	}

	stat := `INSERT INTO source_files (
		filename, size, size_mb, modified, rows, last_upload
	)
	VALUES (
		:filename, :size, :size_mb, :modified, :rows, :last_upload
		)`

	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	tx, _ := db.Beginx()

	_, err = tx.Exec("delete from source_files where filename = $1;", f.Filename)
	if err != nil {
		logs.Add(logs.INFO, err)
		tx.Rollback()
		return
	}

	_, err = tx.NamedExec(stat, f)
	if err != nil {
		logs.Add(logs.INFO, err)
		tx.Rollback()
		return
	} else {
		tx.Commit()
	}

	f.Done = true

}

func (f *FileInfo) SetLastUpload() {

	if db == nil {
		logs.Add(logs.INFO, "no connection to postgres")
		return
	}

	stat := `select last_upload from source_files where filename = $1;`

	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	db.Get(&f.LastUpload, stat, f.Filename)

	f.LastUpload = f.LastUpload.Local().Add(-3 * time.Hour)

}
