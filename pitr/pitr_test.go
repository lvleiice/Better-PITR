package pitr

import (
	"testing"
	
	"gotest.tools/assert"
)

func TestNewPitr(t *testing.T) {
	pitr, err := New(&Config{})
	assert.Assert(t, pitr != nil)
	assert.Assert(t, err == nil)
}