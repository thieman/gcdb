package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/edsrzf/mmap-go"
	"github.com/gamechanger/gcdb/api"
	"github.com/gamechanger/gcdb/filesystem"
	"github.com/gamechanger/gcdb/memory"
)

const (
	prompt = "gcdb> "
)

func initDataFiles() {
	file, err := filesystem.EnsureCurrentDataFile()
	if err != nil {
		panic(err)
	}

	mappedFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}

	mdf := memory.NewMappedDataFile(&mappedFile)
	memory.SetCurrentDataFile(mdf)
	log.Println(mdf)
}

func main() {
	initDataFiles()
	memory.InitializeIndices()

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
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in handleRequest ", r)
		}
	}()

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
		command := api.NewCommandFromInput(buf)
		response, err := api.HandleCommand(command)
		if err != nil {
			conn.Write([]byte(err.Error()))
		} else {
			conn.Write(response)
		}
		conn.Write([]byte{10})
	}
	conn.Close()
}
