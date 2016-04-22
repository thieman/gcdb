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
	commandFlush    = "flush"
	commandFindId   = "findid"
	commandDeleteId = "deleteid"
	commandUpdateId = "updateid"

	responseHi   = "hello frand"
	unrecognized = "Unrecognized command."
)

type Command struct {
	Command string
	Body    *string
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
	case commandHi:
		return []byte(responseHi), nil
	case commandInsert:
		return insert(command)
	case commandFlush:
		return flush(command)
	case commandFindId:
		return findId(command)
	case commandDeleteId:
		return deleteId(command)
	case commandUpdateId:
		return updateId(command)
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
	unmarshaled["_id"] = int(idFloat)

	data, err := json.Marshal(unmarshaled)
	if err != nil {
		return nil, err
	}

	// TODO: Use channels for concurrency control instead of mutex
	locks.GlobalWriteLock.Lock()
	defer locks.GlobalWriteLock.Unlock()
	memory.WriteDocumentToCurrentFile(data)
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
		return nil, errors.New("findid takes an integer ID as its command body")
	}

	idInt, err := strconv.Atoi(*command.Body)
	if err != nil {
		return nil, err
	}
	result, err := memory.CollectionScanCurrentDataFileForId(idInt)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}
	return *result.Document, nil
}

func deleteId(command *Command) ([]byte, error) {
	if command.Body == nil {
		return nil, errors.New("deleteid takes an integer ID as its command body")
	}

	idInt, err := strconv.Atoi(*command.Body)
	if err != nil {
		return nil, err
	}

	// take the lock here so our snapshot doesn't move from under us
	locks.GlobalWriteLock.Lock()
	defer locks.GlobalWriteLock.Unlock()

	result, err := memory.CollectionScanCurrentDataFileForId(idInt)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}

	err = memory.DeleteFromCurrentDataFileAtOffset(result.Offset)
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

	result, err := memory.CollectionScanCurrentDataFileForId(idInt)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf("Id %d not found", idInt))
	}

	err = memory.DeleteFromCurrentDataFileAtOffset(result.Offset)
	if err != nil {
		return nil, err
	}

	memory.WriteDocumentToCurrentFile(data)
	return []byte("OK"), nil
}
