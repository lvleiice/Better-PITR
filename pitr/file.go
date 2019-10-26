package pitr

import (
	"bufio"
	"io"
	"os"
	"path"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"go.uber.org/zap"
)

// searchFiles return matched file with full path
func searchFiles(dir string) ([]string, error) {
	// read all file names
	sortedNames, err := bf.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	binlogFiles := make([]string, 0, len(sortedNames))
	for _, name := range sortedNames {
		fullpath := path.Join(dir, name)
		binlogFiles = append(binlogFiles, fullpath)
	}

	return binlogFiles, nil
}

// filterFiles assume fileNames is sorted by commit time stamp,
// and may filter files not not overlap with [startTS, endTS]
func filterFiles(fileNames []string, startTS int64, endTS int64) ([]string, int64, error) {
	binlogFiles := make([]string, 0, len(fileNames))
	var (
		latestBinlogFile string
		latestFileSize   int64
		allFileSize      int64
	)

	appendFile := func() {
		if latestBinlogFile != "" {
			binlogFiles = append(binlogFiles, latestBinlogFile)
			allFileSize += latestFileSize
			latestBinlogFile = ""
		}
	}

	for _, file := range fileNames {
		ts, fileSize, err := getFirstBinlogCommitTSAndFileSize(file)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		if ts <= startTS {
			latestBinlogFile = file
			latestFileSize = fileSize
			continue
		}

		if ts > endTS && endTS != 0 {
			break
		}

		appendFile()
		latestBinlogFile = file
		latestFileSize = fileSize
	}
	appendFile()

	log.Info("after filter files",
		zap.Strings("files", binlogFiles),
		zap.Int64("all file's size", allFileSize),
		zap.Int64("start tso", startTS),
		zap.Int64("stop tso", endTS))
	return binlogFiles, allFileSize, nil
}

func getFirstBinlogCommitTSAndFileSize(filename string) (int64, int64, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return 0, 0, errors.Annotatef(err, "open file %s error", filename)
	}
	defer fd.Close()

	stat, err := fd.Stat()
	if err != nil {
		return 0, 0, errors.Annotatef(err, "get file stat %s error", filename)
	}
	fileSize := stat.Size()

	_, binlogFileName := path.Split(filename)
	_, ts, err := bf.ParseBinlogName(binlogFileName)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if ts > 0 {
		return ts, fileSize, nil
	}

	// get the first binlog in file
	br := bufio.NewReader(fd)
	binlog, _, err := Decode(br)
	if errors.Cause(err) == io.EOF {
		log.Warn("no binlog find in file", zap.String("filename", filename))
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, errors.Annotatef(err, "decode binlog error")
	}

	return binlog.CommitTs, fileSize, nil
}
