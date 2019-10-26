package pitr

import (
	"fmt"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"go.uber.org/zap"
)

type Event struct {
	schema string
	table  string

	// insert, update or delete
	eventType pb.EventType

	// oldKey used for insert and delete
	oldKey string
	// newKey used for update
	// update may change the primary key or unique key
	newKey string

	cols []*pb.Column

	isDeleted bool
}

func (e *Event) String() string {
	return fmt.Sprintf("{schema: %s, table: %s, eventType: %s, oldKey: %s, newKey: %s, isDeleted: %v}", e.schema, e.table, e.eventType, e.oldKey, e.newKey, e.isDeleted)
}

// Merge two event with same oldKey
// insert + delete = nil, this event should be ignore
// insert + update = insert, and need update oldKey
// update + delete = nil, this event should be ignore
// update + update = update
// delete + insert = update
func (e *Event) Merge(newEvent *Event) {
	log.Info("merge two event", zap.Stringer("old event", e), zap.Stringer("new event", newEvent))
	defer log.Info("after merge", zap.Stringer("event", e))

	if e.eventType == pb.EventType_Insert {
		if newEvent.eventType == pb.EventType_Insert {
			// this should never happened
		} else if newEvent.eventType == pb.EventType_Delete {
			// this row is deleted
			e.isDeleted = true
		} else if newEvent.eventType == pb.EventType_Update {
			// update the newValue
			e.eventType = pb.EventType_Insert
			e.oldToNew(newEvent)
			e.oldKey = newEvent.newKey
		}
	} else if e.eventType == pb.EventType_Update {
		if newEvent.eventType == pb.EventType_Insert {
			// this should not be happen
		} else if newEvent.eventType == pb.EventType_Delete {
			// this row is deleted
			e.isDeleted = true
		} else if newEvent.eventType == pb.EventType_Update {
			// update the newValue
			e.newToNew(newEvent)
		}
	} else if e.eventType == pb.EventType_Delete {
		if newEvent.eventType == pb.EventType_Insert {
			e.newToNew(newEvent)
			e.eventType = pb.EventType_Update
		} else if newEvent.eventType == pb.EventType_Delete {
			// this should never happened
		} else if newEvent.eventType == pb.EventType_Update {
			// this should never happened
		}
	}
}

func (e *Event) oldToNew(newEvent *Event) {
	for i, col := range newEvent.cols {
		e.cols[i].Value = col.ChangedValue
	}
}

func (e *Event) newToNew(newEvent *Event) {
	for i, col := range newEvent.cols {
		e.cols[i].ChangedValue = col.ChangedValue
	}
}
