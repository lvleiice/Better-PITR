package pitr

import (
	"io/ioutil"
	"testing"
	"path"
	"bytes"
	"strings"
	"fmt"

	"github.com/pingcap/check"
	"github.com/BurntSushi/toml"
	"gotest.tools/assert"
)

type testConfigSuite struct{}

var _ = check.Suite(&testConfigSuite{})

func TestParseFromInvalidConfigFile(t *testing.T) {
	yc := struct {
		Dir                    string `toml:"data-dir" json:"data-dir"`
		StartDatetime          string `toml:"start-datetime" json:"start-datetime"`
		StopDatetime           string `toml:"stop-datetime" json:"stop-datetime"`
		StartTSO               int64  `toml:"start-tso" json:"start-tso"`
		StopTSO                int64  `toml:"stop-tso" json:"stop-tso"`
		LogFile                string `toml:"log-file" json:"log-file"`
		LogLevel               string `toml:"log-level" json:"log-level"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"/tmp/pitr",
		"",
		"",
		0,
		0,
		"/tmp/pitr/pitr.log",
		"debug",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	assert.Assert(t, err == nil)

	configFilename := path.Join("/tmp/pitr_test", "pitr_config_invalid.toml")
	err = ioutil.WriteFile(configFilename, buf.Bytes(), 0644)
	assert.Assert(t, err == nil)

	args := []string{
		"--config",
		configFilename,
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	// contained unknown configuration options: unrecognized-option-test
	assert.Assert(t, strings.Contains(err.Error(), "ontained unknown configuration options"))
}

func TestParseFromValidConfigFile(t *testing.T) {
	yc := struct {
		Dir                    string `toml:"data-dir" json:"data-dir"`
		StartDatetime          string `toml:"start-datetime" json:"start-datetime"`
		StopDatetime           string `toml:"stop-datetime" json:"stop-datetime"`
		StartTSO               int64  `toml:"start-tso" json:"start-tso"`
		StopTSO                int64  `toml:"stop-tso" json:"stop-tso"`
		LogFile                string `toml:"log-file" json:"log-file"`
		LogLevel               string `toml:"log-level" json:"log-level"`
	}{
		"/tmp/pitr",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:06",
		0,
		0,
		"/tmp/pitr/pitr.log",
		"debug",
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	assert.Assert(t, err == nil)

	configFilename := path.Join("/tmp/pitr_test", "pitr_config_valid.toml")
	err = ioutil.WriteFile(configFilename, buf.Bytes(), 0644)
	assert.Assert(t, err == nil)

	args := []string{
		"--config",
		configFilename,
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	assert.Assert(t, err == nil)

	err = cfg.validate()
	assert.Assert(t, err == nil)
	cfgStr := cfg.String()
	fmt.Println(cfgStr)
	assert.Assert(t, len(cfgStr) != 0)
}

func TestDateTimeToTSO(t *testing.T) {
	_, err := dateTimeToTSO("123123")
	assert.Assert(t, err != nil)
	_, err = dateTimeToTSO("2019-02-02 15:07:05")
	assert.Assert(t, err == nil)
}
