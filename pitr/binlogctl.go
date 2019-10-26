package pitr

import (
	"io"
	"os"
	"path"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

type myBinlogger struct {
	dir string

	// encoder encodes binlog payload into bytes, and write to file
	encoder binlogfile.Encoder

	lastSuffix uint64
	lastOffset int64

	// file is the lastest file in the dir
	file    *file.LockedFile
	dirLock *file.LockedFile
	mutex   sync.Mutex
}

func OpenMyBinlogger(dirpath string) (*myBinlogger, error) {
	log.Info("open binlogger", zap.String("directory", dirpath))
	var (
		err            error
		lastFileName   string
		lastFileSuffix uint64
		dirLock        *file.LockedFile
		fileLock       *file.LockedFile
	)
	err = os.MkdirAll(dirpath, file.PrivateDirMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// lock directory firstly
	dirLockFile := path.Join(dirpath, ".lock")
	dirLock, err = file.LockFile(dirLockFile, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil && dirLock != nil {
			if err1 := dirLock.Close(); err1 != nil {
				log.Error("failed to unlock", zap.String("directory", dirpath), zap.Error(err1))
			}
		}
	}()

	// ignore file not found error
	names, _ := binlogfile.ReadBinlogNames(dirpath)
	// if no binlog files, we create from index 0, the file name like binlog-0000000000000000-20190101010101
	if len(names) == 0 {
		lastFileName = path.Join(dirpath, binlogfile.BinlogName(0))
		lastFileSuffix = 0
	} else {
		// check binlog files and find last binlog file
		if !binlogfile.IsValidBinlog(names) {
			err = binlogfile.ErrFileContentCorruption
			return nil, errors.Trace(err)
		}

		lastFileName = path.Join(dirpath, names[len(names)-1])
		lastFileSuffix, _, err = binlogfile.ParseBinlogName(names[len(names)-1])
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	log.Info("open and lock binlog file", zap.String("name", lastFileName))
	fileLock, err = file.TryLockFile(lastFileName, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	offset, err := fileLock.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Trace(err)
	}

	binlog := &myBinlogger{
		dir:        dirpath,
		file:       fileLock,
		encoder:    binlogfile.NewEncoder(fileLock, offset),
		dirLock:    dirLock,
		lastSuffix: lastFileSuffix,
		lastOffset: offset,
	}

	return binlog, nil
}

func CloseMyBinlogger(b *myBinlogger) error {
	return b.Close()
}

func (b *myBinlogger) WriteTail(entity *binlog.Entity) (int64, error) {
	payload := entity.Payload

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(payload) == 0 {
		return 0, nil
	}

	curOffset, err := b.encoder.Encode(payload)
	if err != nil {
		log.Error("write local binlog failed", zap.Uint64("suffix", b.lastSuffix), zap.Error(err))
		return 0, errors.Trace(err)
	}

	b.lastOffset = curOffset

	if curOffset < binlogfile.SegmentSizeBytes {
		return curOffset, nil
	}

	err = b.rotate()
	return curOffset, errors.Trace(err)
}

func (b *myBinlogger) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Error("failed to unlock file during closing file", zap.String("name", b.file.Name()), zap.Error(err))
		}
	}

	if b.dirLock != nil {
		if err := b.dirLock.Close(); err != nil {
			log.Error("failed to unlock dir during closing file", zap.String("directory", b.dir), zap.Error(err))
		}
	}

	return nil
}

// rotate creates a new file for append binlog
func (b *myBinlogger) rotate() error {
	filename := binlogfile.BinlogName(b.seq() + 1)
	b.lastSuffix = b.seq() + 1
	b.lastOffset = 0

	fpath := path.Join(b.dir, filename)

	newTail, err := file.LockFile(fpath, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}

	if err = b.file.Close(); err != nil {
		log.Error("failed to unlock during closing file", zap.Error(err))
	}
	b.file = newTail

	b.encoder = binlogfile.NewEncoder(b.file, 0)
	log.Info("segmented binlog file is created", zap.String("path", fpath))
	return nil
}

func (b *myBinlogger) ManualRotate() error {
	return b.rotate()
}

func (b *myBinlogger) seq() uint64 {
	if b.file == nil {
		return 0
	}

	seq, _, err := binlogfile.ParseBinlogName(path.Base(b.file.Name()))
	if err != nil {
		log.Fatal("bad binlog name", zap.String("name", b.file.Name()), zap.Error(err))
	}

	return seq
}
