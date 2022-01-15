package encoding

// Chunk Data block
type Chunk struct {
	Offset    int64
	EndOffset int64
	Size      int64
}

// TableRegion logically splits the imported table data
type TableRegion struct {
	DB    string
	Table string
	File  string

	Chunk Chunk
}

// Size Returns the size of the current block
func (reg *TableRegion) Size() int64 {
	return reg.Chunk.EndOffset - reg.Chunk.Offset
}
