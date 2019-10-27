package pitr

import (
	"gotest.tools/assert"
	"strings"
	"testing"
)

func TestQuoteFunc(t *testing.T) {
	s := quoteSchema("db1", "tb1")
	assert.Assert(t, strings.EqualFold("`db1`.`tb1`", s))

	s = quoteSchema(`db1`, `tb1`)
	assert.Assert(t, strings.EqualFold("`db1`.`tb1`", s))

	s = quoteName("test")
	assert.Assert(t, strings.EqualFold("`test`", s))

	s = escapeName("test`test")
	assert.Assert(t, strings.EqualFold("test``test", s))
}
