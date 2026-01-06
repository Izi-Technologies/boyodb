package boyodb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// Result represents the result of a query.
type Result struct {
	ipcData          []byte
	segmentsScanned  int
	dataSkippedBytes uint64

	// Parsed Arrow data
	schema  *Schema
	batches []*RecordBatch

	// Iteration state
	currentBatch int
	currentRow   int
	closed       bool
}

// Schema represents the Arrow schema.
type Schema struct {
	Fields []Field
}

// Field represents a schema field.
type Field struct {
	Name     string
	Type     DataType
	Nullable bool
}

// DataType represents Arrow data types.
type DataType int

const (
	TypeUnknown DataType = iota
	TypeNull
	TypeBool
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeUint8
	TypeUint16
	TypeUint32
	TypeUint64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeBinary
	TypeDate32
	TypeDate64
	TypeTimestamp
)

// String returns the type name.
func (t DataType) String() string {
	names := []string{
		"unknown", "null", "bool", "int8", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64", "float32", "float64",
		"string", "binary", "date32", "date64", "timestamp",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return "unknown"
}

// RecordBatch represents a batch of rows.
type RecordBatch struct {
	NumRows int
	Columns []Column
}

// Column represents a column of data.
type Column struct {
	Name       string
	Type       DataType
	Nullable   bool
	NullBitmap []byte
	Data       []byte
	Offsets    []int32 // For variable-length types
}

// SegmentsScanned returns the number of segments scanned.
func (r *Result) SegmentsScanned() int {
	return r.segmentsScanned
}

// DataSkippedBytes returns the number of bytes skipped due to pruning.
func (r *Result) DataSkippedBytes() uint64 {
	return r.dataSkippedBytes
}

// Columns returns the column names.
func (r *Result) Columns() []string {
	if r.schema == nil {
		return nil
	}
	names := make([]string, len(r.schema.Fields))
	for i, f := range r.schema.Fields {
		names[i] = f.Name
	}
	return names
}

// ColumnTypes returns the column types.
func (r *Result) ColumnTypes() []DataType {
	if r.schema == nil {
		return nil
	}
	types := make([]DataType, len(r.schema.Fields))
	for i, f := range r.schema.Fields {
		types[i] = f.Type
	}
	return types
}

// RowCount returns the total number of rows across all batches.
func (r *Result) RowCount() int {
	count := 0
	for _, batch := range r.batches {
		count += batch.NumRows
	}
	return count
}

// Next advances to the next row. Returns false when there are no more rows.
func (r *Result) Next() bool {
	if r.closed || len(r.batches) == 0 {
		return false
	}

	// Move to next row
	r.currentRow++

	// Check if we've exhausted the current batch
	for r.currentBatch < len(r.batches) {
		if r.currentRow < r.batches[r.currentBatch].NumRows {
			return true
		}
		// Move to next batch
		r.currentBatch++
		r.currentRow = 0
	}

	return false
}

// Scan copies the current row values into the provided destination variables.
func (r *Result) Scan(dest ...interface{}) error {
	if r.closed {
		return errors.New("result is closed")
	}
	if r.currentBatch >= len(r.batches) {
		return errors.New("no current row")
	}

	batch := r.batches[r.currentBatch]
	if r.currentRow < 0 || r.currentRow >= batch.NumRows {
		return errors.New("invalid row index")
	}

	if len(dest) != len(batch.Columns) {
		return fmt.Errorf("expected %d destination arguments, got %d", len(batch.Columns), len(dest))
	}

	for i, col := range batch.Columns {
		if err := scanValue(&col, r.currentRow, dest[i]); err != nil {
			return fmt.Errorf("column %d (%s): %w", i, col.Name, err)
		}
	}

	return nil
}

// ScanMap copies the current row values into a map.
func (r *Result) ScanMap() (map[string]interface{}, error) {
	if r.closed {
		return nil, errors.New("result is closed")
	}
	if r.currentBatch >= len(r.batches) {
		return nil, errors.New("no current row")
	}

	batch := r.batches[r.currentBatch]
	if r.currentRow < 0 || r.currentRow >= batch.NumRows {
		return nil, errors.New("invalid row index")
	}

	row := make(map[string]interface{})
	for _, col := range batch.Columns {
		val, err := getValue(&col, r.currentRow)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		row[col.Name] = val
	}

	return row, nil
}

// Close releases resources associated with the result.
func (r *Result) Close() error {
	r.closed = true
	r.ipcData = nil
	r.batches = nil
	return nil
}

// ToSlice returns all rows as a slice of maps.
func (r *Result) ToSlice() ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	// Reset to beginning
	r.currentBatch = 0
	r.currentRow = -1

	for r.Next() {
		row, err := r.ScanMap()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// scanValue scans a single value from a column into a destination.
func scanValue(col *Column, row int, dest interface{}) error {
	// Check for NULL
	if col.Nullable && len(col.NullBitmap) > 0 {
		byteIdx := row / 8
		bitIdx := row % 8
		if byteIdx < len(col.NullBitmap) && (col.NullBitmap[byteIdx]&(1<<bitIdx)) == 0 {
			// Value is NULL
			return scanNull(dest)
		}
	}

	return scanNonNull(col, row, dest)
}

// scanNull sets the destination to a nil/zero value.
func scanNull(dest interface{}) error {
	switch d := dest.(type) {
	case *interface{}:
		*d = nil
	case **string:
		*d = nil
	case **int64:
		*d = nil
	case **float64:
		*d = nil
	case **bool:
		*d = nil
	case *string:
		*d = ""
	case *int:
		*d = 0
	case *int64:
		*d = 0
	case *int32:
		*d = 0
	case *float64:
		*d = 0
	case *float32:
		*d = 0
	case *bool:
		*d = false
	default:
		return fmt.Errorf("cannot scan NULL into %T", dest)
	}
	return nil
}

// scanNonNull scans a non-NULL value into the destination.
func scanNonNull(col *Column, row int, dest interface{}) error {
	switch col.Type {
	case TypeBool:
		byteIdx := row / 8
		bitIdx := row % 8
		val := false
		if byteIdx < len(col.Data) {
			val = (col.Data[byteIdx] & (1 << bitIdx)) != 0
		}
		return scanBool(val, dest)

	case TypeInt8:
		if row >= len(col.Data) {
			return errors.New("data out of bounds")
		}
		return scanInt64(int64(int8(col.Data[row])), dest)

	case TypeInt16:
		offset := row * 2
		if offset+2 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := int16(binary.LittleEndian.Uint16(col.Data[offset:]))
		return scanInt64(int64(val), dest)

	case TypeInt32, TypeDate32:
		offset := row * 4
		if offset+4 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := int32(binary.LittleEndian.Uint32(col.Data[offset:]))
		return scanInt64(int64(val), dest)

	case TypeInt64, TypeDate64, TypeTimestamp:
		offset := row * 8
		if offset+8 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := int64(binary.LittleEndian.Uint64(col.Data[offset:]))
		return scanInt64(val, dest)

	case TypeUint8:
		if row >= len(col.Data) {
			return errors.New("data out of bounds")
		}
		return scanInt64(int64(col.Data[row]), dest)

	case TypeUint16:
		offset := row * 2
		if offset+2 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := binary.LittleEndian.Uint16(col.Data[offset:])
		return scanInt64(int64(val), dest)

	case TypeUint32:
		offset := row * 4
		if offset+4 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := binary.LittleEndian.Uint32(col.Data[offset:])
		return scanInt64(int64(val), dest)

	case TypeUint64:
		offset := row * 8
		if offset+8 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		val := binary.LittleEndian.Uint64(col.Data[offset:])
		return scanUint64(val, dest)

	case TypeFloat32:
		offset := row * 4
		if offset+4 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		bits := binary.LittleEndian.Uint32(col.Data[offset:])
		val := math.Float32frombits(bits)
		return scanFloat64(float64(val), dest)

	case TypeFloat64:
		offset := row * 8
		if offset+8 > len(col.Data) {
			return errors.New("data out of bounds")
		}
		bits := binary.LittleEndian.Uint64(col.Data[offset:])
		val := math.Float64frombits(bits)
		return scanFloat64(val, dest)

	case TypeString:
		if len(col.Offsets) <= row+1 {
			return errors.New("offsets out of bounds")
		}
		start := col.Offsets[row]
		end := col.Offsets[row+1]
		if start < 0 || int(end) > len(col.Data) {
			return errors.New("string data out of bounds")
		}
		val := string(col.Data[start:end])
		return scanString(val, dest)

	case TypeBinary:
		if len(col.Offsets) <= row+1 {
			return errors.New("offsets out of bounds")
		}
		start := col.Offsets[row]
		end := col.Offsets[row+1]
		if start < 0 || int(end) > len(col.Data) {
			return errors.New("binary data out of bounds")
		}
		val := col.Data[start:end]
		return scanBytes(val, dest)

	default:
		return fmt.Errorf("unsupported type: %v", col.Type)
	}
}

func scanBool(val bool, dest interface{}) error {
	switch d := dest.(type) {
	case *bool:
		*d = val
	case *interface{}:
		*d = val
	case *string:
		if val {
			*d = "true"
		} else {
			*d = "false"
		}
	default:
		return fmt.Errorf("cannot scan bool into %T", dest)
	}
	return nil
}

func scanInt64(val int64, dest interface{}) error {
	switch d := dest.(type) {
	case *int:
		*d = int(val)
	case *int8:
		*d = int8(val)
	case *int16:
		*d = int16(val)
	case *int32:
		*d = int32(val)
	case *int64:
		*d = val
	case *uint:
		*d = uint(val)
	case *uint8:
		*d = uint8(val)
	case *uint16:
		*d = uint16(val)
	case *uint32:
		*d = uint32(val)
	case *uint64:
		*d = uint64(val)
	case *float32:
		*d = float32(val)
	case *float64:
		*d = float64(val)
	case *string:
		*d = fmt.Sprintf("%d", val)
	case *interface{}:
		*d = val
	default:
		return fmt.Errorf("cannot scan int64 into %T", dest)
	}
	return nil
}

func scanUint64(val uint64, dest interface{}) error {
	switch d := dest.(type) {
	case *int:
		*d = int(val)
	case *int64:
		*d = int64(val)
	case *uint:
		*d = uint(val)
	case *uint64:
		*d = val
	case *float64:
		*d = float64(val)
	case *string:
		*d = fmt.Sprintf("%d", val)
	case *interface{}:
		*d = val
	default:
		return fmt.Errorf("cannot scan uint64 into %T", dest)
	}
	return nil
}

func scanFloat64(val float64, dest interface{}) error {
	switch d := dest.(type) {
	case *float32:
		*d = float32(val)
	case *float64:
		*d = val
	case *string:
		*d = fmt.Sprintf("%g", val)
	case *interface{}:
		*d = val
	default:
		return fmt.Errorf("cannot scan float64 into %T", dest)
	}
	return nil
}

func scanString(val string, dest interface{}) error {
	switch d := dest.(type) {
	case *string:
		*d = val
	case *[]byte:
		*d = []byte(val)
	case *interface{}:
		*d = val
	default:
		return fmt.Errorf("cannot scan string into %T", dest)
	}
	return nil
}

func scanBytes(val []byte, dest interface{}) error {
	switch d := dest.(type) {
	case *[]byte:
		*d = val
	case *string:
		*d = string(val)
	case *interface{}:
		*d = val
	default:
		return fmt.Errorf("cannot scan bytes into %T", dest)
	}
	return nil
}

// getValue returns the value at the given row as an interface{}.
func getValue(col *Column, row int) (interface{}, error) {
	// Check for NULL
	if col.Nullable && len(col.NullBitmap) > 0 {
		byteIdx := row / 8
		bitIdx := row % 8
		if byteIdx < len(col.NullBitmap) && (col.NullBitmap[byteIdx]&(1<<bitIdx)) == 0 {
			return nil, nil
		}
	}

	var dest interface{}
	if err := scanNonNull(col, row, &dest); err != nil {
		return nil, err
	}
	return dest, nil
}

// parseIPC parses Arrow IPC stream format using the Apache Arrow Go reader.
func (r *Result) parseIPC() error {
	if len(r.ipcData) == 0 {
		return nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(r.ipcData))
	if err != nil {
		return fmt.Errorf("open Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			continue
		}

		if r.schema == nil {
			fields := rec.Schema().Fields()
			r.schema = &Schema{Fields: make([]Field, len(fields))}
			for i, f := range fields {
				r.schema.Fields[i] = Field{
					Name:     f.Name,
					Type:     mapArrowType(f.Type),
					Nullable: f.Nullable,
				}
			}
		}

		batch, err := recordToBatch(rec)
		rec.Release()
		if err != nil {
			return fmt.Errorf("decode record: %w", err)
		}
		r.batches = append(r.batches, batch)
	}

	r.currentBatch = 0
	r.currentRow = -1
	return reader.Err()
}

func recordToBatch(rec arrow.Record) (*RecordBatch, error) {
	cols := make([]Column, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		col, err := columnFromArray(rec.Column(i), rec.Schema().Field(i))
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, rec.ColumnName(i), err)
		}
		cols[i] = col
	}

	return &RecordBatch{
		NumRows: int(rec.NumRows()),
		Columns: cols,
	}, nil
}

func columnFromArray(arr arrow.Array, field arrow.Field) (Column, error) {
	col := Column{
		Name:     field.Name,
		Type:     mapArrowType(field.Type),
		Nullable: field.Nullable,
	}

	if nb := arr.NullBitmapBytes(); len(nb) > 0 {
		col.NullBitmap = append([]byte(nil), nb...)
	}

	switch a := arr.(type) {
	case *array.Boolean:
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Int8:
		col.Type = TypeInt8
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Int16:
		col.Type = TypeInt16
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Int32:
		col.Type = TypeInt32
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Int64:
		col.Type = TypeInt64
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Uint8:
		col.Type = TypeUint8
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Uint16:
		col.Type = TypeUint16
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Uint32:
		col.Type = TypeUint32
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Uint64:
		col.Type = TypeUint64
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Float32:
		col.Type = TypeFloat32
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Float64:
		col.Type = TypeFloat64
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.String:
		col.Type = TypeString
		col.Offsets = append([]int32(nil), a.ValueOffsets()...)
		col.Data = append([]byte(nil), a.ValueBytes()...)
	case *array.Binary:
		col.Type = TypeBinary
		col.Offsets = append([]int32(nil), a.ValueOffsets()...)
		col.Data = append([]byte(nil), a.ValueBytes()...)
	case *array.Timestamp:
		col.Type = TypeTimestamp
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Date32:
		col.Type = TypeDate32
		col.Data = appendBuffer(a.Data().Buffers()[1])
	case *array.Date64:
		col.Type = TypeDate64
		col.Data = appendBuffer(a.Data().Buffers()[1])
	default:
		return col, fmt.Errorf("unsupported Arrow type %s", arr.DataType().Name())
	}

	return col, nil
}

func appendBuffer(buf *memory.Buffer) []byte {
	if buf == nil {
		return nil
	}
	return append([]byte(nil), buf.Bytes()...)
}

func mapArrowType(dt arrow.DataType) DataType {
	switch dt.(type) {
	case *arrow.BooleanType:
		return TypeBool
	case *arrow.Int8Type:
		return TypeInt8
	case *arrow.Int16Type:
		return TypeInt16
	case *arrow.Int32Type:
		return TypeInt32
	case *arrow.Int64Type:
		return TypeInt64
	case *arrow.Uint8Type:
		return TypeUint8
	case *arrow.Uint16Type:
		return TypeUint16
	case *arrow.Uint32Type:
		return TypeUint32
	case *arrow.Uint64Type:
		return TypeUint64
	case *arrow.Float32Type:
		return TypeFloat32
	case *arrow.Float64Type:
		return TypeFloat64
	case *arrow.StringType:
		return TypeString
	case *arrow.BinaryType:
		return TypeBinary
	case *arrow.TimestampType:
		return TypeTimestamp
	case *arrow.Date32Type:
		return TypeDate32
	case *arrow.Date64Type:
		return TypeDate64
	default:
		return TypeUnknown
	}
}
