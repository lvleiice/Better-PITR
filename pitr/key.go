package pitr

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

// key is combine with schema, table and pk/uk => schema-name|table-name|pk/uk
func getInsertAndDeleteRowKey(row [][]byte, info *tableInfo) (string, []*pb.Column, error) {
	values := make(map[string]interface{})
	cols := make([]*pb.Column, 0, 10)

	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		cols = append(cols, col)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Info("format value",
			zap.String("col name", col.Name),
			zap.String("mysql type", col.MysqlType),
			zap.Reflect("value", val.GetValue()))
		values[col.Name] = val.GetValue()
	}
	key := fmt.Sprintf("%s|%s|", info.schema, info.table)
	var columns []string
	if len(info.uniqueKeys) != 0 {
		columns = info.uniqueKeys[0].columns
	} else {
		columns = info.columns
	}
	for _, col := range columns {
		key += fmt.Sprintf("%v|", values[col])
	}

	return key, cols, nil
}

// key is combine with schema, table and pk/uk => schema-name|table-name|pk/uk
func getUpdateRowKey(row [][]byte, info *tableInfo) (string, string, []*pb.Column, error) {
	values := make(map[string]interface{})
	changedValues := make(map[string]interface{})
	cols := make([]*pb.Column, 0, 10)

	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", "", nil, errors.Trace(err)
		}
		cols = append(cols, col)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", "", nil, errors.Trace(err)
		}

		_, cVal, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			return "", "", nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		cVal = formatValue(cVal, tp)
		log.Info("format value",
			zap.String("col name", col.Name),
			zap.String("mysql type", col.MysqlType),
			zap.Reflect("value", val.GetValue()),
			zap.Reflect("value", cVal.GetValue()))
		values[col.Name] = val.GetValue()
		changedValues[col.Name] = cVal.GetValue()
	}
	key := fmt.Sprintf("%s|%s|", info.schema, info.table)
	cKey := fmt.Sprintf("%s|%s|", info.schema, info.table)
	var columns []string
	if len(info.uniqueKeys) != 0 {
		columns = info.uniqueKeys[0].columns
	} else {
		columns = info.columns
	}
	for _, col := range columns {
		key += fmt.Sprintf("%v|", values[col])
		cKey += fmt.Sprintf("%v|", changedValues[col])
	}

	return key, cKey, cols, nil
}

func formatValue(value types.Datum, tp byte) types.Datum {
	if value.GetValue() == nil {
		return value
	}

	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		value = types.NewDatum(fmt.Sprintf("%s", value.GetValue()))
	case mysql.TypeEnum:
		value = types.NewDatum(value.GetMysqlEnum().Value)
	case mysql.TypeSet:
		value = types.NewDatum(value.GetMysqlSet().Value)
	case mysql.TypeBit:
		value = types.NewDatum(value.GetMysqlBit())
	}

	return value
}

func getHashKey(schema, table string, ev pb.Event, ddlHandle *DDLHandle) (string, error) {
	tableInfo, err := ddlHandle.GetTableInfo(schema, table)
	if err != nil {
		return "", err
	}
	var key string
	switch ev.GetTp() {
	case pb.EventType_Insert, pb.EventType_Delete:
		key, _, err = getInsertAndDeleteRowKey(ev.GetRow(), tableInfo)
		if err != nil {
			return "", err
		}
	case pb.EventType_Update:
		var cKey string
		var sKey string
		key, cKey, _, err = getUpdateRowKey(ev.GetRow(), tableInfo)
		if err != nil {
			return "", err
		}

		if len(tableInfo.uniqueKeys) == 0 {
			break
		}

		sKey, err = ddlHandle.fetchMapKeyFromDB(key)
		if err != nil {
			return "", nil
		}
		if sKey != "" {
			key = sKey
		}

		if cKey != key {
			sKey, err = ddlHandle.fetchMapKeyFromDB(cKey)
			if err != nil {
				return "", err
			}

			if sKey == "" {
				err = ddlHandle.insertMapKeyFromDB(cKey, key)
				if err != nil {
					return "", err
				}
			}
		}
	default:
		panic("unreachable")
	}

	return key, nil
}
