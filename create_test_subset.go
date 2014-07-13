package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"os"
	"strings"
)

var m map[string]bool
var s []string

func zipLineIter(filename string, ch chan string) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	z, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(z)

	for line, _, err := r.ReadLine(); err == nil; line, _, err = r.ReadLine() {
		ch <- string(line)
	}
	close(ch)
	f.Close()
}

func searchRels(gzfile string) {
	log.Printf("searching for related records\n")
	ch := make(chan string, 1024*1024)
	go zipLineIter(gzfile, ch)
	count := 0
	for line := range ch {
		count++
		if count > 5000000 {
			//	break
		}
		if count%100000000 == 0 {
			log.Printf("searchRels has scanned %dM lines, triples: %d\n", count/1000000, len(s))
		}
		lines := strings.Split(line, "\t")
		if len(lines) < 3 {
			continue
		}
		if _, ok := m[lines[0]]; ok {
			s = append(s, line)
		} else if _, ok := m[lines[2]]; ok {
			s = append(s, line)
		}
	}
}

func searchInstances(searchstring, gzfile string) {
	log.Printf("searching for instances containing %s\n", searchstring)
	ch := make(chan string, 1024*1024)
	go zipLineIter(gzfile, ch)
	count := 0
	for line := range ch {
		count++
		if count > 400000000 {
			break
		}
		if count%100000000 == 0 {
			log.Printf("searchInstances has scanned %dM lines, found %d instances\n", count/1000000, len(m))
		}
		if len(line) == 0 {
			continue
		}
		if strings.Contains(line, searchstring) &&
			strings.Contains(line, "type.type.instance") {
			lines := strings.Split(line, "\t")
			if len(lines) < 3 {
				break
			}
			m[lines[2]] = true
		}
	}
}

func filterNodes() {
	log.Println("filtering to just nodes and types...")
	newS := []string{}
	for i, _ := range s {
		lines := strings.Split(s[i], "\t")
		if len(lines) < 3 {
			break
		}
		if strings.Contains(lines[2], "<http://rdf.freebase.com/ns/m.") {
			newS = append(newS, s[i])
			m[lines[2]] = true
		}
		if strings.Contains(lines[0], "<http://rdf.freebase.com/ns/m.") {
			newS = append(newS, s[i])
			m[lines[0]] = true
		}
	}
	s = newS
}

func outputSubset(outfile string) {
	log.Println("writing to file...")
	out, err := os.Create(outfile)
	if err != nil {
		log.Println(err)
	}
	defer out.Close()

	for _, x := range s {
		_, err := io.WriteString(out, x+"\n")
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	if len(os.Args) < 4 {
		log.Fatal("not enough args given. usage:\n create_test_subset freebase-dump.gz <searchstring> <outputfile>\n")
	}
	freebase := os.Args[1]
	searchstring := os.Args[2]
	outfile := os.Args[3]

	m = map[string]bool{}
	s = []string{}

	searchInstances(searchstring, freebase)
	searchRels(freebase)
	filterNodes()
	searchRels(freebase)
	filterNodes()
	searchRels(freebase)
	outputSubset(outfile)
}
