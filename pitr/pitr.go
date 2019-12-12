package pitr

import (
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
)

// historyDDLHandler handles the DDL works we need to do before compressing to
// binlog file, it'll prepare the schema at StartTSO.
type historyDDLHandler struct {
	ddlJobs []*model.Job
}

func (h *historyDDLHandler) execute() (err error) {
	return ddlHandle.ExecuteHistoryDDLs(h.ddlJobs)
}

// PITR is the main part of the merge binlog tool.
type PITR struct {
	cfg *Config
}

// New creates a PITR object.
func New(cfg *Config) (*PITR, error) {
	log.Info("New PITR", zap.Stringer("config", cfg))
	return &PITR{
		cfg: cfg,
	}, nil
}

// Process runs the main procedure.
func (r *PITR) Process() error {
	files, err := searchFiles(r.cfg.Dir)
	if err != nil {
		return errors.Annotate(err, fmt.Sprintf("search files in directory %s failed", r.cfg.Dir))
	}
	if len(files) == 0 {
		return errors.Annotate(err, fmt.Sprintf("no file is searched in directory %s", r.cfg))
	}

	files, fileSize, err := filterFiles(files, r.cfg.StartTSO, r.cfg.StopTSO)
	if err != nil {
		return errors.Annotate(err, "filterFiles failed")
	}
	if len(files) == 0 {
		return errors.Annotate(err, fmt.Sprintf("no files remained between the time interval [%d, %d]", r.cfg.StartTSO, r.cfg.StopTSO))
	}

	startTSO := r.cfg.StartTSO
	if startTSO == 0 {
		startTSO, _, err = getFirstBinlogCommitTSAndFileSize(files[0])
		if err != nil {
			return errors.Annotate(err, "get first binlog commit ts failed")
		}
	}

	historyDDLs, err := r.fetchDDLBeforeStartTSO(startTSO)
	if err != nil {
		return errors.Trace(err)
	}
	merge, err := NewMerge(files, fileSize, r.cfg.OutputDir)
	if err != nil {
		return errors.Trace(err)
	}
	defer merge.Close(r.cfg.reserveTempDir)
	if err = historyDDLs.execute(); err != nil {
		return err
	}
	if noEventIsFound, err := merge.Map(startTSO, r.cfg.StopTSO); err != nil {
		return errors.Trace(err)
	} else if noEventIsFound {
		log.Info(fmt.Sprintf("no event is found between [%d, %d]", startTSO, r.cfg.StopTSO))
		return nil
	}
	if err = historyDDLs.execute(); err != nil {
		return errors.Trace(err)
	}

	return merge.Reduce()
}

// Close closes the PITR object.
func (r *PITR) Close() error {
	return nil
}

func (r *PITR) loadHistoryDDLJobs(beginTS int64) ([]*model.Job, error) {
	tiStore, err := createTiStore(r.cfg.PDURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		tiStore.Close()
	}()

	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allJobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(allJobs, func(i, j int) bool {
		return allJobs[i].BinlogInfo.SchemaVersion < allJobs[j].BinlogInfo.SchemaVersion
	})

	// only get ddl job which finished ts is less than begin ts
	jobs := make([]*model.Job, 0, 10)
	for _, job := range allJobs {
		if int64(job.BinlogInfo.FinishedTS) < beginTS {
			jobs = append(jobs, job)
		} else {
			log.Info("ignore history ddl job", zap.Reflect("job", job))
		}
	}

	return jobs, nil
}

func createTiStore(urls string) (kv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := store.Register("tikv", tikv.Driver{}); err != nil {
		return nil, errors.Trace(err)
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

func (r *PITR) fetchDDLBeforeStartTSO(startTSO int64) (historyDDLs historyDDLHandler, err error) {
	switch {
	case len(r.cfg.PDURLs) != 0:
		historyDDLs.ddlJobs, err = r.loadHistoryDDLJobs(startTSO)
		err = errors.Annotate(err, "load history ddls")
	}
	return historyDDLs, errors.Trace(err)
}
