package config

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"time"
)

var (
	cfg   Config
	Debug bool

	// C:/Users/Public/projects/processing
	WorkDir string

	//go:embed databases.conf
	databases embed.FS

	NumCPU int

	// READ_GOROUTINES       = 5  // 5
	// WRITE_CSV_GOROUTINES  = 5  // 5
	// WRITE_PSQL_GOROUTINES = 10 // 10
)

type Storage string
type Application string

const (
	PSQL       Storage     = "PSQL"
	Clickhouse Storage     = "CH"
	File       Storage     = "FILE"
	PROCESSING Application = "processing"
	TRANSFER   Application = "transfer"
)

type (
	Config struct {
		Async       bool
		Application Application
		File_config string

		File_logs   string `json:"file_logs"`
		File_errors string `json:"file_errors"`

		Clickhouse DatabaseConnection `json:"clickhouse"`
		PSQL       DatabaseConnection `json:"psql"`

		Registry    Registry   `json:"registry"`
		Tariffs     ImportData `json:"tariffs"`
		Crypto      ImportData `json:"crypto"`
		Rates       ImportData `json:"rates"`
		Decline     ImportData `json:"decline"`
		Detailed    ExportData `json:"detailed"`
		SummaryInfo ExportData `json:"summary_info"`
		Summary     ExportData `json:"summary"`
	}

	DatabaseConnection struct {
		Usage    bool
		Name     string `json:"name"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
	}

	Registry struct {
		Storage       Storage   `json:"storage"`
		DateFrom      time.Time `json:"date_from"`
		DateTo        time.Time `json:"date_to"`
		Merchant_id   []int     `json:"merchant_id"`
		Merchant_name []string  `json:"merchant_name"`
		Filename      string    `json:"filename"`
	}

	ImportData struct {
		Storage  Storage `json:"storage"`
		Filename string  `json:"filename"`
	}

	ExportData struct {
		Usage    bool    `json:"usage"`
		Storage  Storage `json:"storage"`
		Filename string  `json:"filename"`
	}
)

func init() {
	dir, _ := os.Getwd()
	WorkDir = strings.ReplaceAll(dir, "\\", "/")
	Debug = strings.Contains(WorkDir, "processing")
	NumCPU = runtime.NumCPU()
}

func New(app string, async bool, file_config string) {
	cfg = Config{
		Application: Application(app),
		Async:       async,
		File_config: file_config,
	}
}

func Get() Config {
	return cfg
}

func Load() (err error) {

	err = cfg.ReadDatabasesConfig()
	if err != nil {
		return err
	}

	if cfg.File_config != "" {

		cfg.ReadConfigFile()

	} else if Debug {

		cfg.File_config = string(cfg.Application) + "/config.conf"

		err := cfg.ReadConfigFile()
		if err != nil {
			return err
		}

	} else {
		return errors.New("не обнаружен файл конфигурации")
	}

	cfg.SetDBUsage()

	os.Remove(cfg.File_logs)
	os.Remove(cfg.File_errors)

	return nil

}

func (c *Config) ReadDatabasesConfig() error {

	file, err := databases.Open("databases.conf")
	if err != nil {
		return err
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, c)
	if err != nil {
		return err
	}

	return nil

}

func (c *Config) ReadConfigFile() error {

	file, err := os.OpenFile(c.File_config, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, c)
	if err != nil {
		return err
	}

	return nil

}

func (c *Config) SetDBUsage() {

	c.Clickhouse.Usage = c.Registry.Storage == Clickhouse

	c.PSQL.Usage = c.Tariffs.Storage == PSQL ||
		c.Decline.Storage == PSQL ||
		c.Crypto.Storage == PSQL ||
		c.Rates.Storage == PSQL ||
		c.Summary.Storage == PSQL ||
		c.SummaryInfo.Storage == PSQL ||
		c.Detailed.Storage == PSQL

}
