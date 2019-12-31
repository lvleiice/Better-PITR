package pitr

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

const (
	toolName   = "tidb-binlog-pitr"
	timeFormat = "2006-01-02 15:04:05"

	defaultOutputDir string = "./new_binlog"
)

// Config is the main configuration for the retore tool.
type Config struct {
	*flag.FlagSet `toml:"-" json:"-"`
	Dir           string `toml:"data-dir" json:"data-dir"`
	OutputDir     string `toml:"output-dir" json:"output-dir"`
	StartDatetime string `toml:"start-datetime" json:"start-datetime"`
	StopDatetime  string `toml:"stop-datetime" json:"stop-datetime"`
	StartTSO      int64  `toml:"start-tso" json:"start-tso"`
	StopTSO       int64  `toml:"stop-tso" json:"stop-tso"`

	PDURLs string `toml:"pd-urls" json:"pd-urls"`

	LogFile  string `toml:"log-file" json:"log-file"`
	LogLevel string `toml:"log-level" json:"log-level"`

	reserveTempDir bool `toml:"reserve-tmpdir" json:"reserve-tmpdir"`

	configFile   string
	printVersion bool
}

// NewConfig creates a Config object.
func NewConfig() *Config {
	c := &Config{}
	c.FlagSet = flag.NewFlagSet(toolName, flag.ContinueOnError)
	fs := c.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Usage of %s:", toolName))
		fs.PrintDefaults()
	}
	fs.StringVar(&c.Dir, "data-dir", "", "drainer data directory path")
	fs.StringVar(&c.OutputDir, "output-dir", defaultOutputDir, "output directory path")
	fs.StringVar(&c.StartDatetime, "start-datetime", "", "recovery from start-datetime, empty string means starting from the beginning of the first file")
	fs.StringVar(&c.StopDatetime, "stop-datetime", "", "recovery end in stop-datetime, empty string means never end.")
	fs.Int64Var(&c.StartTSO, "start-tso", 0, "similar to start-datetime but in pd-server tso format")
	fs.Int64Var(&c.StopTSO, "stop-tso", 0, "similar to stop-datetime, but in pd-server tso format")
	fs.StringVar(&c.LogFile, "log-file", "", "log file path")
	fs.StringVar(&c.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&c.configFile, "config", "", "[REQUIRED] path to configuration file")
	fs.StringVar(&c.PDURLs, "pd-urls", "", "a comma separated list of PD endpoints")
	fs.BoolVar(&c.reserveTempDir, "reserve-tmpdir", false, "reserve temp dir")
	fs.BoolVar(&c.printVersion, "V", false, "print pitr version info")
	return c
}

func (c *Config) String() string {
	cfgBytes, err := json.Marshal(c)
	if err != nil {
		log.Error("marshal config failed", zap.Error(err))
	}

	return string(cfgBytes)
}

// Parse parses keys/values from command line flags and toml configuration file.
func (c *Config) Parse(args []string) (err error) {
	// Parse first to get config file
	perr := c.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	if c.printVersion {
		fmt.Println(version.GetRawVersionInfo())
		os.Exit(0)
	}

	if c.configFile != "" {
		// Load config file if specified
		if err := c.configFromFile(c.configFile); err != nil {
			fmt.Println("read from config file failed")
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options
	if err := c.FlagSet.Parse(args); err != nil {
		return errors.Trace(err)
	}
	if len(c.FlagSet.Args()) > 0 {
		fmt.Printf("'%s' is not a valid flag", c.FlagSet.Arg(0))
		return errors.Errorf("'%s' is not a valid flag", c.FlagSet.Arg(0))
	}

	// replace with environment vars
	if err := flags.SetFlagsFromEnv(toolName, c.FlagSet); err != nil {
		return errors.Trace(err)
	}

	if c.StartDatetime != "" {
		c.StartTSO, err = dateTimeToTSO(c.StartDatetime)
		if err != nil {
			fmt.Printf("startDatetime %s can not be converted to timestamp oracle format", c.StartDatetime)
			return errors.Trace(err)
		}
	} else if c.StartTSO == 0 {
		fmt.Println("Note: StartTSO is set to 0 automatically")
	}

	if c.StopDatetime != "" {
		c.StopTSO, err = dateTimeToTSO(c.StopDatetime)
		if err != nil {
			fmt.Printf("stopDatetime %s can not be converted to timestamp oracle format", c.StopDatetime)
			return errors.Trace(err)
		}
	} else if c.StopTSO == 0 {
		c.StopTSO = math.MaxInt64
		fmt.Println("Note: Stop TSO is set to MaxInt64 automatically")
	}
	err = c.validate()
	if err != nil {
		return err
	}
	fmt.Printf("\nconfig:\ninputDataDir: %s\noutputDataDir: %s\n[startDatetime, stopDatetime]=[%s,%s]\n[startTso, stopTso]=[%d,%d]\n\n", c.Dir, c.OutputDir, c.StartDatetime, c.StopDatetime, c.StartTSO, c.StopTSO)
	return
}

func (c *Config) configFromFile(path string) error {
	return util.StrictDecodeFile(path, toolName, c)
}

func (c *Config) validate() error {
	if c.Dir == "" {
		fmt.Println("data-dir is empty")
		return errors.New("data-dir is empty")
	}

	return nil
}

func dateTimeToTSO(dateTimeStr string) (int64, error) {
	t, err := time.ParseInLocation(timeFormat, dateTimeStr, time.Local)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return int64(oracle.ComposeTS(t.Unix()*1000, 0)), nil
}
