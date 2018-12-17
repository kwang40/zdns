package file

import (
	"bufio"
	"os"
	"sync"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"../../../zdns"
)

type FileInputHandler struct {
	filepath string
}

func (h *FileInputHandler) Initialize(conf *zdns.GlobalConf) {
	h.filepath = conf.InputFilePath
}

func (h *FileInputHandler) FeedChannel(in chan<- interface{}, wg *sync.WaitGroup, zonefileInput bool) error {
	defer close(in)
	defer (*wg).Done()

	var f *os.File
	if h.filepath == "" || h.filepath == "-" {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(h.filepath)
		if err != nil {
			log.Fatal("unable to open input file:", err.Error())
		}
	}
	if zonefileInput {
		tokens := dns.ParseZone(f, ".", h.filepath)
		for t := range tokens {
			in <- t
		}
	} else {
		s := bufio.NewScanner(f)
		for s.Scan() {
			in <- s.Text()
		}
		if err := s.Err(); err != nil {
			log.Fatal("input unable to read file", err)
		}
	}
	return nil
}

type FileOutputHandler struct {
	filepath string
}

func (h *FileOutputHandler) Initialize(conf *zdns.GlobalConf) {
	h.filepath = conf.OutputFilePath
}

func (h *FileOutputHandler) WriteResults(results <-chan string, wg *sync.WaitGroup, stdOutput bool) error {
	defer (*wg).Done()

	var f *os.File
	if stdOutput || h.filepath == "" || h.filepath == "-" {
		f = os.Stdout
	} else {
		var err error
		f, err = os.OpenFile(h.filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal("unable to open output file:", err.Error())
		}
		defer f.Close()
	}
	for n := range results {
		if _, err := f.WriteString(n + "\n"); err != nil {
			if stdOutput {
				log.Fatal("Problem at stdout channel")
			}
			log.Fatal("Problem writing", n, err.Error())
		}
	}
	return nil
}


// register handlers
func init() {
	in := new(FileInputHandler)
	zdns.RegisterInputHandler("file", in)

	out := new(FileOutputHandler)
	zdns.RegisterOutputHandler("file", out)
}
