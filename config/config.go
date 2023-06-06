// This package defines a common config struct which can be used by any subsystem within slick.
package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Debug                    bool
	RootDir                  string
	AckWaitTimeMs            int64
	GroupMessageWaitTimeMs   int64
	PrivateMessageWaitTimeMs int64
	LookupTimeoutMs          int64
	RequestTimeoutMs         int64
	LoggingPrefix            string
	writer                   io.Writer
}

func (c Config) Logger(source string) *zap.SugaredLogger {
	var p string
	if source == "" {
		p = c.LoggingPrefix
	} else {
		p = fmt.Sprintf("%s:%s", c.LoggingPrefix, source)
	}

	level := zapcore.InfoLevel
	if c.Debug {
		level = zapcore.DebugLevel
	}
	opts := []zap.Option{
		zap.Fields(zap.String("source", p)),
	}

	de := zap.NewDevelopmentEncoderConfig()
	fileEncoder := zapcore.NewJSONEncoder(de)
	consoleEncoder := zapcore.NewConsoleEncoder(de)
	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, zapcore.AddSync(c.writer), level),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)
	logger := zap.New(core, opts...)
	sugar := logger.Sugar()
	return sugar
}

type Option func(*Config)

func WithDebug(d bool) Option {
	return func(c *Config) {
		c.Debug = d
	}
}

func WithRootDir(d string) Option {
	return func(c *Config) {
		c.RootDir = d
	}
}

func WithAckWaitTimeMs(n int64) Option {
	return func(c *Config) {
		c.AckWaitTimeMs = n
	}
}

func WithLoggingPrefix(p string) Option {
	return func(c *Config) {
		c.LoggingPrefix = p
	}
}

func WithGroupMessageWaitTimeMs(n int64) Option {
	return func(c *Config) {
		c.GroupMessageWaitTimeMs = n
	}
}

func WithPrivateMessageWaitTimeMs(n int64) Option {
	return func(c *Config) {
		c.PrivateMessageWaitTimeMs = n
	}
}

func NewConfig(opts ...Option) *Config {
	c := &Config{
		Debug:                    os.Getenv("DEBUG") == "1",
		AckWaitTimeMs:            5000,
		GroupMessageWaitTimeMs:   1000,
		PrivateMessageWaitTimeMs: 500,
		LookupTimeoutMs:          1000,
		RequestTimeoutMs:         5000,
		LoggingPrefix:            "",
		RootDir:                  ".",

		writer: nil,
	}
	for _, o := range opts {
		o(c)
	}

	writer := &lumberjack.Logger{
		Filename:   filepath.Join(c.RootDir, "out.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   // days
		Compress:   true, // disabled by default
	}
	c.writer = writer
	return c
}
