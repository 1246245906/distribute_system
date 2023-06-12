package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type LevelType int

const (
	ERROR LevelType = iota
	WARN
	INFO
	DEBUG
)

type Config struct {
	LogConfig struct {
		Level   string `json:"lever"`
		LogPath string `json:"logpath"`
		Prefix  string `json:"prefix"`
	} `json:"Logger"`
}

type Logger struct {
	*log.Logger
	level LevelType
}

func NewLogger(configPath string) *Logger {
	l := Logger{}
	l.init(configPath)
	return &l
}

func (logger *Logger) init(configPath string) {
	configfile, err := os.Open(configPath)
	defer configfile.Close()
	if err != nil {
		logger.level = INFO
		logger.Logger = log.New(os.Stdout, "", log.LstdFlags)
		log.Printf("Could not open config file %s, use default config!", configPath)
		return
	}
	configdecoder := json.NewDecoder(configfile)
	var config Config
	configdecoder.Decode(&config)
	switch strings.ToLower(config.LogConfig.Level) {
	case "error":
		logger.level = ERROR
	case "warn":
		logger.level = WARN
	case "info":
		logger.level = INFO
	case "debug":
		logger.level = DEBUG
	}
	logpath := config.LogConfig.LogPath
	now := time.Now().Format("20060102_150405")
	logpath_s := strings.Split(logpath, ".")
	reallogpath := fmt.Sprintf("%s-%s.%s", strings.Join(logpath_s[0:len(logpath_s)-1], "."), now, logpath_s[len(logpath_s)-1])
	prefix := config.LogConfig.Prefix
	logfile, err := os.Create(reallogpath)
	if err != nil {
		log.Printf("Could not create log file %s, use default config!", reallogpath)
		logger.Logger = log.New(os.Stdout, prefix, log.LstdFlags)
		return
	}
	logger.Logger = log.New(logfile, prefix, log.LstdFlags)
}

func (l *Logger) error(format string, v ...interface{}) {
	if l.level >= ERROR {
		l.Printf("[ERROR] "+format, v...)
	}
}

func (l *Logger) warn(format string, v ...interface{}) {
	if l.level >= WARN {
		l.Printf("[WARN] "+format, v...)
	}
}

func (l *Logger) info(format string, v ...interface{}) {
	if l.level >= INFO {
		l.Printf("[INFO] "+format, v...)
	}
}

func (l *Logger) debug(format string, v ...interface{}) {
	if l.level >= DEBUG {
		l.Printf("[DEBUG] "+format, v...)
	}
}
