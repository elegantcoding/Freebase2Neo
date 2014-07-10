package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

func main() {
	freebase := os.Args[1]
	subset := os.Args[2]
	m := make(map[string]bool)

	sf, err := os.Open(subset)
	if err != nil {
		panic(err)
	}
	defer sf.Close()
	sfr := bufio.NewReader(sf)

	for line, _, err := sfr.ReadLine(); err != io.EOF; line, _, err = sfr.ReadLine() {
		lines := bytes.Split(line, []byte("\t"))
		if len(lines) < 3 {
			break
		}
		m[string(lines[2])] = true
	}

	//fmt.Println("done building map...")

	ff, err := os.Open(freebase)
	if err != nil {
		panic(err)
	}
	defer ff.Close()
	ffz, err := gzip.NewReader(ff)
	if err != nil {
		panic(err)
	}
	ffr := bufio.NewReader(ffz)

	for line, _, err := ffr.ReadLine(); err != io.EOF; line, _, err = ffr.ReadLine() {
		lines := bytes.Split(line, []byte("\t"))
		if len(lines) < 3 {
			continue
		}
		if _, ok := m[string(lines[0])]; ok {
			fmt.Println(string(line))
		} else if _, ok := m[string(lines[2])]; ok {
			fmt.Println(string(line))
		}
	}
}
