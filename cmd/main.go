// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"go.uber.org/zap"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "net/http/pprof"

	"fmt"
	"github.com/lvleiice/Better-PITR/pitr"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pitr.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	if err := util.InitLogger(cfg.LogLevel, cfg.LogFile); err != nil {
		fmt.Println("Failed to initialize log", err.Error())
		os.Exit(1)
	}

	version.PrintVersionInfo("PITR")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	r, err := pitr.New(cfg)
	if err != nil {
		fmt.Printf("pitr process failed, please check the log file [%s] for detailed message.", cfg.LogFile)
		log.Fatal("create pitr failed", zap.Error(err))
		os.Exit(1)
	}

	go func() {
		sig := <-sc
		fmt.Println("got signal to exit. [signal=", sig, "]")
		r.Close()
		os.Exit(0)
	}()

	if err := r.Process(); err != nil {
		fmt.Printf("PITR failed, please check the log file [%s] for detailed message.", cfg.LogFile)
		log.Error("pitr processing failed", zap.Error(err))
		os.Exit(1)
	}
	if err := r.Close(); err != nil {
		fmt.Printf("pitr process failed, please check the log file [%s] for detailed message.", cfg.LogFile)
		log.Fatal("close pitr failed", zap.Error(err))
		os.Exit(1)
	}
	fmt.Printf("pitr process successfully, please check the outpur directory %s", cfg.OutputDir)
}
