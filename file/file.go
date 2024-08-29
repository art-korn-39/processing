package file

import (
	"app/logs"
	"app/querrys"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	REG_BOF      = "BOF registry"
	REG_PROVIDER = "provider registry"
	CRYPTO       = "crypto"
	DECLINE      = "decline"
)

type FileInfo struct {
	Filename   string    `db:"filename"`
	Category   string    `db:"category"`
	Size       int       `db:"size"`
	Size_mb    int       `db:"size_mb"`
	Modified   time.Time `db:"modified"`
	Rows       int       `db:"rows"`
	LastUpload time.Time `db:"last_upload"`

	done bool
	mu   sync.Mutex
}

func GetFiles(filenames []string, category string, extension string) []*FileInfo {

	files := make([]*FileInfo, 0, len(filenames))

	for _, f := range filenames {

		if strings.Contains(f, "~$") || filepath.Ext(f) != extension {
			continue
		}

		// file, err := os.OpenFile(f, os.O_RDONLY, os.FileMode(0400))
		// if err != nil {
		// 	err = fmt.Errorf("os.OpenFile() %v", err)
		// 	logs.Add(logs.ERROR, err)
		// 	continue
		// }
		// defer file.Close()

		// stat, err := file.Stat()
		// if err != nil {
		// 	err = fmt.Errorf("file.Stat() %v", err)
		// 	logs.Add(logs.ERROR, err)
		// 	continue
		// }

		// fileInfo := &FileInfo{
		// 	Filename: f,
		// 	Category: category,
		// 	Size:     int(stat.Size()),
		// 	Size_mb:  int(stat.Size()) / 1024000,
		// 	Modified: stat.ModTime(),
		// }

		fileInfo, err := New(f, category)
		if err != nil {
			logs.Add(logs.ERROR, err)
			continue
		}

		files = append(files, fileInfo)

	}

	return files

}

func New(filename, category string) (*FileInfo, error) {

	file, err := os.OpenFile(filename, os.O_RDONLY, os.FileMode(0400))
	if err != nil {
		err = fmt.Errorf("os.OpenFile() %v", err)
		//logs.Add(logs.ERROR, err)
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("file.Stat() %v", err)
		//logs.Add(logs.ERROR, err)
		return nil, err
	}

	fileInfo := &FileInfo{
		Filename: filename,
		Category: category,
		Size:     int(stat.Size()),
		Size_mb:  int(stat.Size()) / 1024000,
		Modified: stat.ModTime(),
	}

	return fileInfo, nil

}

func (f *FileInfo) GetLastUpload(db *sqlx.DB) {

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

func (f *FileInfo) InsertIntoDB(db *sqlx.DB, duration time.Duration) {

	if db == nil {
		logs.Add(logs.FATAL, "no connection to postgres")
		return
	}

	if duration != 0 {
		ticker := time.NewTicker(duration)
		<-ticker.C
		ticker.Stop()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return
	}

	stat := querrys.Stat_Insert_source_files()

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

	f.done = true

	logs.Add(logs.MAIN, fmt.Sprint("Записан в postgres: ", filepath.Base(f.Filename)))

}
