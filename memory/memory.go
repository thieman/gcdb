package memory

import (
	"encoding/binary"
	"encoding/json"
	"log"

	"github.com/edsrzf/mmap-go"
	"github.com/gamechanger/gcdb/locks"
)

const (
	dataStartOffset = uint32(1 + 4 + 8)
)

type MappedDataFile struct {
	initialized bool
	offset      uint32
	version     uint64
	mappedFile  *mmap.MMap
}

type IdUnmarshaller struct {
	Id int `json:"_id"`
}

type Document struct {
	Document *[]byte
	Offset   uint32 // offset of the entire document, not the data segment
	deleted  bool
	version  uint64
}

var currentDataFile *MappedDataFile

// Here's the jank-ass format for the data files
// First byte: 0 if file has been initialized, 1 otherwise
// Next eight bytes: uint64 storing current op version
// Next four bytes: uint32 storing latest write offset in file
// Errythang else: Dem datas

// And the format for dem datas is:
// First byte: 0 if document is current, 1 if deleted
// Next eight bytes: uint64 of last valid op version for this doc if it's deleted now
// First four bytes: uint32 storing length of data segment
// Following bytes: data segment

func NewMappedDataFile(mappedFile *mmap.MMap) *MappedDataFile {
	new := &MappedDataFile{initialized: false, offset: 0, mappedFile: mappedFile}
	new.Initialize()
	return new
}

func SetCurrentDataFile(mdf *MappedDataFile) {
	locks.StopTheWorld()
	defer locks.UnstopTheWorld()
	currentDataFile = mdf
}

func WriteDocumentToCurrentFile(data []byte) {
	// Data format is four bytes for uint32 describing
	// size of following payload.
	headerBytes := make([]byte, 1+8+4)
	binary.BigEndian.PutUint32(headerBytes[1+8:], uint32(len(data)))
	currentDataFile.WriteBytes(headerBytes)
	currentDataFile.WriteBytes(data)
}

// Delete the document at the given offset
// Right now this is being chained together by a scan at the caller level
// Seems kinda dirty /shrug
func DeleteFromCurrentDataFileAtOffset(offset uint32) error {
	versionBytes := make([]byte, 8)
	currentDataFile.WriteBytesAtOffset([]byte{1}, offset)
	binary.BigEndian.PutUint64(versionBytes, currentDataFile.version)
	currentDataFile.WriteBytesAtOffset(versionBytes, offset+1)
	currentDataFile.IncrementVersion()
	return nil
}

func CollectionScanCurrentDataFileForId(id int) (*Document, error) {
	// TODO: Think we can parallelize the JSON encoding part of this more

	resultChannel := make(chan *Document, 50)
	// need a buffer here since receiver might be dead by the
	// time we tell it to close. channel will get GC'd in that case anyway
	stopChannel := make(chan bool, 1)
	defer func() {
		stopChannel <- true
	}()

	go currentDataFile.CollectionScan(resultChannel, stopChannel)
	idUnmarshalStruct := IdUnmarshaller{} // faster, deserialize less, reuse struct
	for doc := range resultChannel {
		err := json.Unmarshal(*doc.Document, &idUnmarshalStruct)
		if err != nil {
			return nil, err
		}
		if idUnmarshalStruct.Id == id {
			return doc, nil
		}
	}
	return nil, nil
}

func FlushCurrentFile() error {
	return currentDataFile.Flush()
}

func (mdf *MappedDataFile) Initialize() {
	initByte := mdf.ReadBytesAtOffset(1, 0)
	if (*initByte)[0] != 0 { // previously initialized
		mdf.initialized = true
		offsetBytes := mdf.ReadBytesAtOffset(4, 1)
		mdf.offset = binary.BigEndian.Uint32(*offsetBytes)
		versionBytes := mdf.ReadBytesAtOffset(8, 5)
		mdf.version = binary.BigEndian.Uint64(*versionBytes)
		return
	}
	mdf.offset = dataStartOffset
	mdf.version = uint64(1)
	mdf.WriteOffsetHeader()
	mdf.WriteVersionHeader()
	mdf.WriteBytesAtOffset([]byte{1}, 0)
	mdf.initialized = true
	mdf.Flush()
}

func (mdf *MappedDataFile) Flush() error {
	return mdf.mappedFile.Flush()
}

func (mdf *MappedDataFile) IncrementVersion() {
	mdf.version += uint64(1)
	mdf.WriteVersionHeader()
}

func (mdf *MappedDataFile) WriteVersionHeader() {
	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, mdf.version)
	mdf.WriteBytesAtOffset(versionBytes, 5)
}

func (mdf *MappedDataFile) WriteOffsetHeader() {
	offsetBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(offsetBytes, mdf.offset)
	mdf.WriteBytesAtOffset(offsetBytes, 1)
}

func (mdf *MappedDataFile) ReadBytesAtOffset(numBytes, offset uint32) *[]byte {
	new := make([]byte, numBytes)
	for idx := range new {
		new[idx] = (*mdf.mappedFile)[offset+uint32(idx)]
	}
	return &new
}

func (mdf *MappedDataFile) WriteBytesAtOffset(data []byte, offset uint32) {
	for idx := range data {
		(*mdf.mappedFile)[offset+uint32(idx)] = data[idx]
	}
}

func (mdf *MappedDataFile) WriteBytes(data []byte) {
	mdf.WriteBytesAtOffset(data, mdf.offset)
	mdf.offset += uint32(len(data))
	mdf.WriteOffsetHeader()
}

func (mdf *MappedDataFile) ReadDocumentAtOffset(offset uint32) (document *Document, nextOffset uint32) {
	headerBytes := mdf.ReadBytesAtOffset(1+8+4, offset)
	docVersion := binary.BigEndian.Uint64((*headerBytes)[1:9])
	docLength := binary.BigEndian.Uint32((*headerBytes)[1+8:])
	doc := Document{
		Document: mdf.ReadBytesAtOffset(docLength, offset+1+8+4),
		Offset:   offset,
		deleted:  (*headerBytes)[0] == 1,
		version:  docVersion}
	return &doc, offset + 1 + 8 + 4 + docLength
}

func (mdf *MappedDataFile) CollectionScan(outputChannel chan *Document, stopChannel chan bool) {
	// This is basically taking a snapshot at the time the scan starts
	// We will not scan any documents inserted after we record this
	// Additionally, any documents deleted before the current DB version
	// will not be returned
	currentOffset := dataStartOffset
	currentVersion := currentDataFile.version
	stopOffset := mdf.offset
	for currentOffset < stopOffset {
		select {
		case <-stopChannel:
			log.Println("CollectionScan got stop")
			return
		default:
			document, nextOffset := mdf.ReadDocumentAtOffset(currentOffset)
			currentOffset = nextOffset
			if document.deleted && document.version < currentVersion {
				continue
			}
			outputChannel <- document
		}
	}
	close(outputChannel)
}
