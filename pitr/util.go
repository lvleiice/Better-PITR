package pitr

import (
	"fmt"
	"strings"
)

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func quoteDB(dbName string) string {
	return fmt.Sprintf("`%s`", escapeName(dbName))
}

func quoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}
