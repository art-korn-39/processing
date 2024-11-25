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

	//go:embed cloud.conf
	cloud embed.FS

	//go:embed crm.conf
	crm embed.FS

	NumCPU int
)

type Storage string
type Application string

const (
	PSQL       Storage = "PSQL"
	Clickhouse Storage = "CH"
	File       Storage = "FILE"
	AWS        Storage = "AWS"
	CRM        Storage = "CRM"
)

type (
	Config struct {
		Application Application
		File_config string

		File_logs   string `json:"file_logs"`
		File_errors string `json:"file_errors"`

		Routine_task bool `json:"routine_task"`
		Full_loading bool `json:"full_loading"`

		Settings Settings `json:"settings"`

		// подключения к базам
		Clickhouse DatabaseConnection `json:"clickhouse"`
		PSQL       DatabaseConnection `json:"psql"`
		AWS        CloudConnection    `json:"aws"`
		CRM        ODataConnection    `json:"crm"`

		Registry          Registry   `json:"registry"` //входящие данные (bof,crm,aws)
		Tariffs           ImportData `json:"tariffs"`
		Crypto            ImportData `json:"crypto"`
		Provider_registry ImportData `json:"provider_registry"` //!!!!
		Decline           ImportData `json:"decline"`
		Dragonpay         ImportData `json:"dragonpay"`
		Detailed          ExportData `json:"detailed"`
		SummaryInfo       ExportData `json:"summary_info"`
		Summary           ExportData `json:"summary"`
	}

	Settings struct {
		Guid     []string `json:"guid"`
		Handbook string   `json:"handbook"`
	}

	DatabaseConnection struct {
		Usage    bool
		Name     string `json:"name"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Key      string `json:"key"`
		Secret   string `json:"secret"`
	}

	CloudConnection struct {
		Usage  bool
		Key    string `json:"key"`
		Secret string `json:"secret"`
		Bucket string `json:"bucket"`
		Region string `json:"region"`
	}

	ODataConnection struct {
		Usage    bool
		URL      string `json:"url"`
		Login    string `json:"login"`
		Password string `json:"password"`
	}

	Registry struct {
		Storage       Storage   `json:"storage"`
		DateFrom      time.Time `json:"date_from"`
		DateTo        time.Time `json:"date_to"`
		Merchant_id   []int     `json:"merchant_id"`
		Merchant_name []string  `json:"merchant_name"`
		Provider_id   []int     `json:"provider_id"`
		Provider_name []string  `json:"provider_name"`
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

func New(app, file_config string) {
	cfg = Config{
		Application: Application(app),
		File_config: file_config,
	}
}

func Get() Config {
	return cfg
}

func Load() (err error) {

	err = cfg.ReadEmbedFile(databases, "databases.conf")
	if err != nil {
		return err
	}

	err = cfg.ReadEmbedFile(cloud, "cloud.conf")
	if err != nil {
		return err
	}

	err = cfg.ReadEmbedFile(crm, "crm.conf")
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

	// clean, there can be previous launches
	if !cfg.Routine_task {
		os.Remove(cfg.File_logs)
		os.Remove(cfg.File_errors)
	}

	return nil

}

func (c *Config) ReadEmbedFile(fs embed.FS, name string) error {

	file, err := fs.Open(name)
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

	// c.PSQL.Usage = c.Tariffs.Storage == PSQL ||
	// 	c.Decline.Storage == PSQL ||
	// 	c.Crypto.Storage == PSQL ||
	// 	c.Rates.Storage == PSQL ||
	// 	c.Dragonpay.Storage == PSQL ||
	// 	c.Summary.Storage == PSQL ||
	// 	c.SummaryInfo.Storage == PSQL ||
	// 	c.Detailed.Storage == PSQL

	c.PSQL.Usage = true

	c.AWS.Usage = c.Registry.Storage == AWS
	if c.AWS.Usage {
		c.Clickhouse.Usage = true
		//c.PSQL.Usage = true
	}

	c.CRM.Usage = c.Registry.Storage == CRM
	if c.CRM.Usage {
		c.Clickhouse.Usage = false
		//c.PSQL.Usage = true
	}

}
