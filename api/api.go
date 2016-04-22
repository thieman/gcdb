package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gamechanger/gcdb/locks"
	"github.com/gamechanger/gcdb/memory"
)

const (
	commandHi       = "hi"
	commandInsert   = "insert"
	commandFindId   = "findid"
	commandFindAll  = "findall"
	commandGetMore  = "getmore"
	commandDeleteId = "deleteid"
	commandUpdateId = "updateid"
	commandIndex    = "index"
	commandFlush    = "flush"
	commandHelp     = "help"

	responseHi   = "hello frand"
	unrecognized = "Unrecognized command."
)

var useIndicesForQuery = false
var responseHelp string
var nextCursorId int
var activeCursorOffsets map[int]uint32

type Command struct {
	Command string
	Body    *string
}

func init() {
	responseHelp = "Command List\n"
	for _, s := range []string{commandHi, commandInsert, commandFindId, commandFindAll, commandGetMore, commandDeleteId, commandUpdateId, commandIndex, commandFlush} {
		responseHelp += s
		responseHelp += "\n"
	}
	activeCursorOffsets = make(map[int]uint32)
	nextCursorId = 1
}

// Initialize and return the ID of a new cursor
func NewCursor() int {
	newId := nextCursorId
	activeCursorOffsets[newId] = memory.DataStartOffset
	nextCursorId++
	return newId
}

func updateCursor(cursorId int, newOffset uint32) {
	activeCursorOffsets[cursorId] = newOffset
}

func NewCommandFromInput(buf []byte) *Command {
	s := string(bytes.Trim(buf, string([]byte{0, 10})))
	pieces := strings.Split(s, " ")
	var c *Command
	if len(pieces) < 2 {
		c = &Command{Command: pieces[0], Body: nil}
	} else {
		joined := strings.Join(pieces[1:], " ")
		c = &Command{Command: pieces[0], Body: &joined}
	}
	return c
}

func HandleCommand(command *Command) ([]byte, error) {
	switch command.Command {
	case commandHelp:
		return []byte(responseHelp), nil
	case commandHi:
		return []byte(responseHi), nil
	case commandInsert:
		return insert(command)
	case commandFlush:
		return flush(command)
	case commandFindId:
		return findId(command)
	case commandFindAll:
		return findAll(command)
	case commandGetMore:
		return getMore(command)
	case commandDeleteId:
		return deleteId(command)
	case commandUpdateId:
		return updateId(command)
	case commandIndex:
		return toggleIndices(command)
	default:
		return nil, errors.New(unrecognized)
	}
}

func insert(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("insert takes a JSON object as its command body")
	}
	unmarshaled := make(map[string]interface{})
	err := json.Unmarshal([]byte(*command.Body), &unmarshaled)
	if err != nil {
		return nil, err
	}

	var id interface{}
	var idFloat float64
	var ok bool
	if id, ok = unmarshaled["_id"]; !ok {
		return nil, errors.New("Document must contain an integer _id field")
	}
	if idFloat, ok = id.(float64); !ok {
		return nil, errors.New("Document must contain an integer _id field")
	}
	idInt := int(idFloat)
	unmarshaled["_id"] = idInt

	if memory.IdExistsInIndex(idInt) {
		return nil, errors.New(fmt.Sprintf("Id %d violates unique constraint, another document already has this Id", idInt))
	}

	data, err := json.Marshal(unmarshaled)
	if err != nil {
		return nil, err
	}

	// TODO: Use channels for concurrency control instead of mutex
	locks.GlobalWriteLock.Lock()
	defer locks.GlobalWriteLock.Unlock()
	memory.WriteDocumentToCurrentFile(idInt, data)
	return []byte("OK"), nil
}

func flush(command *Command) ([]byte, error) {
	err := memory.FlushCurrentFile()
	if err != nil {
		return nil, err
	}
	return []byte("OK"), nil
}

func findId(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("findid takes a document's integer ID as its command body")
	}

	idInt, err := strconv.Atoi(*command.Body)
	if err != nil {
		return nil, err
	}

	var result *memory.Document
	if useIndicesForQuery == false {
		result, err = memory.CollectionScanCurrentDataFileForId(idInt)
	} else {
		result, err = memory.IndexScanCurrentDataFileForId(idInt)
	}
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}
	return *result.Document, nil
}

func findAll(command *Command) ([]byte, error) {
	// TODO: Put the version control stuff in cursors too
	locks.GlobalCursorLock.Lock()
	defer locks.GlobalCursorLock.Unlock()
	cursorId := NewCursor()
	return []byte(strconv.Itoa(cursorId)), nil
}

func getMore(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("findid takes a cursor's integer ID as its command body")
	}

	idInt, err := strconv.Atoi(*command.Body)
	if err != nil {
		return nil, err
	}

	locks.GlobalCursorLock.Lock()
	defer locks.GlobalCursorLock.Unlock()

	offset, ok := activeCursorOffsets[idInt]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Could not find cursor with Id %d", idInt))
	}
	result, err := memory.CollectionScanCurrentDataFileFromOffset(offset, 20)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("cursor exhausted")
	}

	var idx int
	output := make([]byte, 0)
	for idx = range result {
		output = append(output, *result[idx].Document...)
		output = append(output, byte(10))
	}
	updateCursor(idInt, (*result[idx]).NextOffset)
	return output, nil
}

func deleteId(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("deleteid takes a document's integer ID as its command body")
	}

	idInt, err := strconv.Atoi(*command.Body)
	if err != nil {
		return nil, err
	}

	// take the lock here so our snapshot doesn't move from under us
	locks.GlobalWriteLock.Lock()
	defer locks.GlobalWriteLock.Unlock()

	var result *memory.Document
	if useIndicesForQuery == false {
		result, err = memory.CollectionScanCurrentDataFileForId(idInt)
	} else {
		result, err = memory.IndexScanCurrentDataFileForId(idInt)
	}

	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}

	err = memory.DeleteDocumentFromCurrentDataFileAtOffset(idInt, result.Offset)
	if err != nil {
		return nil, err
	}
	return []byte("OK"), nil
}

// Update is implemented as a delete followed by an insert
// Does not currently upsert; if doc does not already exist
// then the entire update will fail
func updateId(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("updateid takes an integer ID and a new JSON doc as its command body")
	}

	pieces := strings.Split(*command.Body, " ")
	if len(pieces) < 2 {
		return nil, errors.New("updateid takes an integer ID and a new JSON doc as its command body")
	}

	idInt, err := strconv.Atoi(pieces[0])
	if err != nil {
		return nil, err
	}

	unmarshaled := make(map[string]interface{})
	err = json.Unmarshal([]byte(strings.Join(pieces[1:], " ")), &unmarshaled)
	if err != nil {
		return nil, err
	}

	var id interface{}
	var idFloat float64
	var ok bool
	if id, ok = unmarshaled["_id"]; !ok {
		return nil, errors.New("Document must contain an integer _id field")
	}
	if idFloat, ok = id.(float64); !ok {
		return nil, errors.New("Document must contain an integer _id field")
	}
	if int(idFloat) != idInt {
		return nil, errors.New("New document must have same _id as document being updated")
	}

	data, err := json.Marshal(unmarshaled)
	if err != nil {
		return nil, err
	}

	locks.GlobalWriteLock.Lock()
	defer locks.GlobalWriteLock.Unlock()

	var result *memory.Document
	if useIndicesForQuery == false {
		result, err = memory.CollectionScanCurrentDataFileForId(idInt)
	} else {
		result, err = memory.IndexScanCurrentDataFileForId(idInt)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}

	err = memory.DeleteDocumentFromCurrentDataFileAtOffset(idInt, result.Offset)
	if err != nil {
		return nil, err
	}

	memory.WriteDocumentToCurrentFile(idInt, data)
	return []byte("OK"), nil
}

func toggleIndices(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("index takes either 'on' or 'off' as its body")
	}

	if *command.Body == "on" {
		useIndicesForQuery = true
		return []byte("INDICES ON"), nil
	} else if *command.Body == "off" {
		useIndicesForQuery = false
		return []byte("INDICES OFF"), nil
	}
	return nil, errors.New("index takes either 'on' or 'off' as its body")
}
