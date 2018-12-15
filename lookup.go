/*
 * ZDNS Copyright 2016 Regents of the University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package zdns

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

type routineMetadata struct {
	Names  int
	Status map[Status]int
}
type MiekgAnswer struct {
	Ttl     uint32 `json:"ttl,omitempty"`
	Type    string `json:"type,omitempty"`
	RrType  uint16
	Class   string `json:"class,omitempty"`
	RrClass uint16
	Name    string `json:"name,omitempty"`
	Answer  string `json:"answer,omitempty"`
}

type DNSFlags struct {
	Response           bool `json:"response"`
	Opcode             int  `json:"opcode"`
	Authoritative      bool `json:"authoritative"`
	Truncated          bool `json:"truncated"`
	RecursionDesired   bool `json:"recursion_desired"`
	RecursionAvailable bool `json:"recursion_available"`
	Authenticated      bool `json:"authenticated"`
	CheckingDisabled   bool `json:"checking_disabled"`
	ErrorCode          int  `json:"error_code"`
}

// result to be returned by scan of host
type MiekgResult struct {
	Answers     []interface{} `json:"answers"`
	Additional  []interface{} `json:"additionals"`
	Authorities []interface{} `json:"authorities"`
	Protocol    string        `json:"protocol"`
	Flags       DNSFlags      `json:"flags"`
}


func GetDNSServers(path string) ([]string, error) {
	c, err := dns.ClientConfigFromFile(path)
	if err != nil {
		return []string{}, err
	}
	var servers []string
	for _, s := range c.Servers {
		if s[0:1] != "[" && strings.Contains(s, ":") {
			s = "[" + s + "]"
		}
		full := strings.Join([]string{s, c.Port}, ":")
		servers = append(servers, full)
	}
	return servers, nil
}

func parseAlexa(line string) (string, int) {
	s := strings.SplitN(line, ",", 2)
	rank, err := strconv.Atoi(s[0])
	if err != nil {
		log.Fatal("Malformed Alexa Top Million file")
	}
	return s[1], rank
}

func makeName(name string, prefix string) (string, bool) {
	if prefix == "" {
		return name, false
	} else {
		return strings.Join([]string{prefix, name}, ""), true
	}
}

func doLookup(g *GlobalLookupFactory, gc *GlobalConf, input <-chan interface{}, output chan<- string, outStdChan chan<- string, resultChannel chan<- Result, metaChan chan<- routineMetadata, wg *sync.WaitGroup, threadID int) error {
	f, err := (*g).MakeRoutineFactory(threadID)
	if err != nil {
		log.Fatal("Unable to create new routine factory", err.Error())
	}
	var metadata routineMetadata
	metadata.Status = make(map[Status]int)
	for genericInput := range input {
		var res Result
		var innerRes interface{}
		var trace []interface{}
		var status Status
		var err error
		l, err := f.MakeLookup()
		if err != nil {
			log.Fatal("Unable to build lookup instance", err)
		}
		if (*g).ZonefileInput() {
			length := len(genericInput.(*dns.Token).RR.Header().Name)
			if length == 0 {
				continue
			}
			res.Name = genericInput.(*dns.Token).RR.Header().Name[0 : length-1]
			res.Class = dns.Class(gc.Class).String()
			switch typ := genericInput.(*dns.Token).RR.(type) {
			case *dns.NS:
				ns := strings.ToLower(typ.Ns)
				res.Nameserver = ns[:len(ns)-1]
			}
			innerRes, status, err = l.DoZonefileLookup(genericInput.(*dns.Token))
		} else {
			line := genericInput.(string)
			var changed bool
			var rawName string
			var rank int
			if gc.AlexaFormat == true {
				rawName, rank = parseAlexa(line)
				res.AlexaRank = rank
			} else {
				rawName = line
			}
			lookupName, changed := makeName(rawName, gc.NamePrefix)
			if changed {
				res.AlteredName = lookupName
			}
			res.Name = rawName
			res.Class = dns.Class(gc.Class).String()
			innerRes, trace, status, err = l.DoLookup(lookupName)
		}
		res.Timestamp = time.Now().Format(gc.TimeFormat)
		if status != STATUS_NO_OUTPUT {
			res.Status = string(status)
			res.Data = innerRes
			res.Trace = trace
			if err != nil {
				res.Error = err.Error()
			}

			resultChannel <- res
			jsonRes, err := json.Marshal(res)
			if err != nil {
				log.Fatal("Unable to marshal JSON result", err)
			}
			output <- string(jsonRes)
			if len(gc.StdOutModules) != 0 {
				res, ok := innerRes.(MiekgResult)
				if ok {
					answers := res.Answers
					for i := range(answers) {
						answer, answerOk := answers[i].(MiekgAnswer)
						if !answerOk {
							continue
						}
						if (gc.StdOutModules[answer.Type] || gc.StdOutModules["ANY"]) && len(answer.Answer) > 0 && answer.Answer != "<nil>" {
							outStdChan<-answer.Answer
							break
						}
					}
				}
			}

		}

		metadata.Names++
		metadata.Status[status]++
	}
	metaChan <- metadata
	(*wg).Done()
	return nil
}

func aggregateMetadata(c <-chan routineMetadata) Metadata {
	var meta Metadata
	meta.Status = make(map[string]int)
	for m := range c {
		meta.Names += m.Names
		for k, v := range m.Status {
			meta.Status[string(k)] += v
		}
	}
	return meta
}

func DoLookups(g *GlobalLookupFactory, c *GlobalConf) error {
	// DoLookup:
	//	- n threads that do processing from in and place results in out
	//	- process until inChan closes, then wg.done()
	// Once we processing threads have all finished, wait until the
	// output and metadata threads have completed
	inChan := make(chan interface{})
	outChan := make(chan string)
	outStdChan := make(chan string)
	outRedisChan := make(chan Result)
	metaChan := make(chan routineMetadata, c.Threads)
	var routineWG sync.WaitGroup
	var routineWGstdOut sync.WaitGroup
	var routineRedisWG sync.WaitGroup

	inHandler := GetInputHandler(c.InputHandler)
	outHandler := GetOutputHandler(c.OutputHandler)
	redisHandler := new(RedisOutputHandler)
	inHandler.Initialize(c)
	outHandler.Initialize(c)

	// Use handlers to populate the input and output/results channel
	go inHandler.FeedChannel(inChan, &routineWG, (*g).ZonefileInput())
	go outHandler.WriteResults(outChan, &routineWG, false)
	go outHandler.WriteResults(outStdChan, &routineWGstdOut, true)
	if c.RedisServerUrl != "" {
		go redisHandler.WriteResults(outRedisChan, &routineRedisWG)
	}
	routineWG.Add(2)
	if len(c.StdOutModules) != 0 {
		go outHandler.WriteResults(outStdChan, &routineWG, true)
		routineWG.Add(1)
	}
	routineWGstdOut.Add(1)
	routineRedisWG.Add(1)

	// create pool of worker goroutines
	var lookupWG sync.WaitGroup
	lookupWG.Add(c.Threads)
	startTime := time.Now().Format(c.TimeFormat)
	for i := 0; i < c.Threads; i++ {
		go doLookup(g, c, inChan, outChan, outStdChan, outRedisChan, metaChan, &lookupWG, i)
	}
	lookupWG.Wait()
	close(outChan)
	close(outStdChan)
	close(outRedisChan)
	close(metaChan)
	routineWG.Wait()
	routineWGstdOut.Wait()
	routineRedisWG.Wait()
	if c.MetadataFilePath != "" {
		// we're done processing data. aggregate all the data from individual routines
		metaData := aggregateMetadata(metaChan)
		metaData.StartTime = startTime
		metaData.EndTime = time.Now().Format(c.TimeFormat)
		metaData.NameServers = c.NameServers
		metaData.Retries = c.Retries
		// Seconds() returns a float. However, timeout is passed in as an integer
		// command line argument, so there should be no loss of data when casting
		// back to an integer here.
		metaData.Timeout = int(c.Timeout.Seconds())
		metaData.Conf = c
		// add global lookup-related metadata
		// write out metadata
		var f *os.File
		if c.MetadataFilePath == "-" {
			f = os.Stderr
		} else {
			var err error
			f, err = os.OpenFile(c.MetadataFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Fatal("unable to open metadata file:", err.Error())
			}
			defer f.Close()
		}
		j, err := json.Marshal(metaData)
		if err != nil {
			log.Fatal("unable to JSON encode metadata:", err.Error())
		}
		f.WriteString(string(j))
	}
	return nil
}


type RedisOutputHandler struct {
	client *redis.Client
}

func (h *RedisOutputHandler) Initialize(conf GlobalConf) {
	h.client = redis.NewClient(&redis.Options{
		Addr:     conf.RedisServerUrl,
		Password: conf.RedisServerPass,
		DB:       conf.RedisServerDB,
	})
}

func (h *RedisOutputHandler) WriteResults(results <-chan Result, wg *sync.WaitGroup) error {
	defer (*wg).Done()

	for r := range results {
		res, ok := r.Data.(MiekgResult)
		if !ok {
			o, err := json.Marshal(res)
			if err != nil {
				log.Fatal(err)
			}
			log.Warn("unable to parse result: ", string(o))
		}

		for _, a := range res.Answers {
			var key, domain string
			if miekgAnswer, ok := a.(MiekgAnswer); ok {
				if miekgAnswer.Type == dns.TypeToString[dns.TypeA] || miekgAnswer.Type == dns.TypeToString[dns.TypeAAAA] {
					key = miekgAnswer.Answer
					domain = miekgAnswer.Name
				}
			} else {
			//} else if mxAnswer, ok := a.(miekg.MXAnswer); ok {
			//	key = mxAnswer.Answer.Answer
			//	domain = mxAnswer.Answer.Name
			//} else {
				log.Warn("unimplemented answer type (not MiekgAnswer or MXAnswer")
			}

			var value []string
			redisValue, err := h.client.Get(key).Result()
			if err == redis.Nil { // no key found
				value = make([]string, 0)
			} else if err != nil {
				log.Fatal("unable to get key:", err)
			} else {
				err = json.Unmarshal([]byte(redisValue), &value)
				if err != nil {
					log.Error("error unmarshalling redis string:", err)
				}
			}

			value = append(value, domain)
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				log.Error("error marshalling redis:", err)
			}
			err = h.client.Set(key, string(jsonBytes[:]), 0).Err()
			if err != nil {
				log.Error("unable to set redis key:", err)
			}
		}

	}
	return nil
}

