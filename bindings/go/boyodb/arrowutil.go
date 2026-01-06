package boyodb

import (
	"bytes"
	"errors"
	"io"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// DecodeRows decodes Arrow IPC bytes into a slice of maps (col -> value).
// For large result sets, prefer streaming over maps to avoid allocations.
func DecodeRows(ipcBytes []byte) ([]map[string]interface{}, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(ipcBytes))
	if err != nil {
		return nil, err
	}
	defer rdr.Release()

	var out []map[string]interface{}
	for rdr.Next() {
		rec := rdr.Record()
		schema := rec.Schema()
		for row := 0; row < int(rec.NumRows()); row++ {
			rowMap := make(map[string]interface{}, len(schema.Fields()))
			for colIdx, field := range schema.Fields() {
				col := rec.Column(colIdx)
				switch arr := col.(type) {
				case *array.Uint64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.Int64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.Float64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.String:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				default:
					return nil, errors.New("DecodeRows: unsupported column type")
				}
			}
			out = append(out, rowMap)
		}
	}
	return out, rdr.Err()
}

// DecodeCountSumAvg expects a result with columns count, sum (int64), optional avg (float64).
type CountSumAvg struct {
	Count uint64
	Sum   int64
	Avg   float64
}

// DecodeCountSumAvg decodes a single-row aggregate response.
func DecodeCountSumAvg(ipcBytes []byte) (CountSumAvg, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(ipcBytes), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return CountSumAvg{}, err
	}
	defer rdr.Release()
	if !rdr.Next() {
		return CountSumAvg{}, errors.New("no rows")
	}
	rec := rdr.Record()
	if rec.NumRows() == 0 {
		return CountSumAvg{}, errors.New("empty record batch")
	}
	var out CountSumAvg
	for i, field := range rec.Schema().Fields() {
		name := field.Name
		col := rec.Column(i)
		switch name {
		case "count":
			arr := col.(*array.Uint64)
			if arr.IsNull(0) {
				return out, errors.New("count is null")
			}
			out.Count = arr.Value(0)
		case "sum":
			arr := col.(*array.Int64)
			if arr.IsNull(0) {
				return out, errors.New("sum is null")
			}
			out.Sum = arr.Value(0)
		case "avg":
			arr := col.(*array.Float64)
			if arr.IsNull(0) {
				return out, errors.New("avg is null")
			}
			out.Avg = arr.Value(0)
		}
	}
	return out, rdr.Err()
}

// DecodeRowsStream streams Arrow IPC rows to a callback to avoid large allocations.
// The callback receives a map for each row; return an error to stop early.
func DecodeRowsStream(r io.Reader, cb func(map[string]interface{}) error) error {
	rdr, err := ipc.NewReader(r)
	if err != nil {
		return err
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		schema := rec.Schema()
		for row := 0; row < int(rec.NumRows()); row++ {
			rowMap := make(map[string]interface{}, len(schema.Fields()))
			for colIdx, field := range schema.Fields() {
				col := rec.Column(colIdx)
				switch arr := col.(type) {
				case *array.Uint64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.Int64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.Float64:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				case *array.String:
					if arr.IsNull(row) {
						rowMap[field.Name] = nil
					} else {
						rowMap[field.Name] = arr.Value(row)
					}
				default:
					return errors.New("DecodeRowsStream: unsupported column type")
				}
			}
			if err := cb(rowMap); err != nil {
				return err
			}
		}
	}
	return rdr.Err()
}
