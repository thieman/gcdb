package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"strings"

	"github.com/edsrzf/mmap-go"
	"github.com/gamechanger/gcdb/filesystem"
	"github.com/gamechanger/gcdb/memory"
)

const (
	prompt       = "gcdb> "
	commandHi    = "hi"
	responseHi   = "hello frand"
	unrecognized = "Unrecognized command."
)

type Command struct {
	Command string
	Body    *string
}

func main() {
	file, err := filesystem.EnsureCurrentDataFile()
	if err != nil {
		panic(err)
	}

	mappedFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}

	_ = memory.NewMappedDataFile(&mappedFile)

	l, err := net.Listen("tcp", "localhost:19999")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	log.Println("gcdb listening on port 19999")

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		conn.Write([]byte(prompt))
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed")
				conn.Close()
				return
			}
			panic(err)
		}
		response := handleCommand(buf)
		conn.Write(response)
		conn.Write([]byte{10})
	}
	conn.Close()
}

func handleCommand(buf []byte) []byte {
	command := NewCommand(buf)
	if command.Command == commandHi {
		return []byte(responseHi)
	}
	return []byte(unrecognized)
}

func NewCommand(buf []byte) *Command {
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
