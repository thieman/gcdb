package memory

import (
	"encoding/binary"

	"github.com/edsrzf/mmap-go"
)

type MappedDataFile struct {
	initialized bool
	offset      int
	mappedFile  *mmap.MMap
}

// Here's the jank-ass format for the data files
// First byte: 0 if file has been initialized, 1 otherwise
// Next four bytes: uint32 storing latest write offset in file
// Errythang else: Dem datas

// And the format for dem datas is:
// First two bytes: uint16 storing length of data segment
// Following bytes: data segment

func NewMappedDataFile(mappedFile *mmap.MMap) *MappedDataFile {
	new := &MappedDataFile{initialized: false, offset: 0, mappedFile: mappedFile}
	new.Initialize()
	return new
}

func (mdf *MappedDataFile) Initialize() {
	initByte := mdf.ReadBytesAtOffset(1, 0)
	if (*initByte)[0] != 0 { // previously initialized
		mdf.initialized = true
		offsetBytes := mdf.ReadBytesAtOffset(4, 1)
		mdf.offset = int(binary.BigEndian.Uint32(*offsetBytes))
		return
	}
	mdf.offset = 5
	mdf.WriteOffsetHeader()
	mdf.WriteBytesAtOffset([]byte{1}, 0)
	mdf.initialized = true
	mdf.Flush()
}

func (mdf *MappedDataFile) Flush() {
	mdf.mappedFile.Flush()
}

func (mdf *MappedDataFile) WriteOffsetHeader() {
	offsetBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(offsetBytes, uint32(mdf.offset))
	mdf.WriteBytesAtOffset(offsetBytes, 1)
}

func (mdf *MappedDataFile) ReadBytesAtOffset(numBytes, offset int) *[]byte {
	new := make([]byte, numBytes)
	for idx := range new {
		new[idx] = (*mdf.mappedFile)[offset+idx]
	}
	return &new
}

func (mdf *MappedDataFile) WriteBytesAtOffset(data []byte, offset int) {
	for idx := range data {
		(*mdf.mappedFile)[offset+idx] = data[idx]
	}
}

func (mdf *MappedDataFile) WriteBytes(data []byte) {
	mdf.WriteBytesAtOffset(data, mdf.offset)
	mdf.offset += len(data)
}
