package pitr

import (
	"testing"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"gotest.tools/assert"
)

func TestMerge(t *testing.T) {
	insertEvent := &Event{
		eventType: pb.EventType_Insert,
		oldKey:    "1",
		cols: []*pb.Column{
			{
				Value: []byte("1"),
			},
		},
	}

	updateEvent := &Event{
		eventType: pb.EventType_Update,
		oldKey:    "1",
		newKey:    "2",
		cols: []*pb.Column{
			{
				Value:        []byte("1"),
				ChangedValue: []byte("2"),
			},
		},
	}

	deleteEvent := &Event{
		eventType: pb.EventType_Delete,
		oldKey:    "2",
		cols: []*pb.Column{
			{
				Value: []byte("2"),
			},
		},
	}

	insertEvent.Merge(updateEvent)
	assert.Assert(t, insertEvent.eventType == pb.EventType_Insert)
	assert.Assert(t, insertEvent.oldKey == updateEvent.newKey)

	insertEvent.Merge(deleteEvent)
	assert.Assert(t, insertEvent.isDeleted == true)

	updateEvent.Merge(deleteEvent)
	assert.Assert(t, updateEvent.eventType == pb.EventType_Delete)

	deleteEvent.Merge(insertEvent)
	assert.Assert(t, deleteEvent.eventType == pb.EventType_Update)
}
