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

package main

import (
	"flag"
	_"fmt"
	_ "github.com/kwang40/zdns/iohandlers/file"
	_ "github.com/kwang40/zdns/modules/alookup"
	_ "github.com/kwang40/zdns/modules/axfr"
	_ "github.com/kwang40/zdns/modules/dmarc"
	_ "github.com/kwang40/zdns/modules/miekg"
	_ "github.com/kwang40/zdns/modules/mxlookup"
	_ "github.com/kwang40/zdns/modules/nslookup"
	_ "github.com/kwang40/zdns/modules/spf"
	"os"
	"log"
	"sync"
	"bufio"
	"strings"
	"strconv"
	"github.com/go-redis/redis"
	"encoding/json"
	"fmt"
)

var (
	redisServerUrl  string
	redisServerPass string
	redisServerDB   int
)

func outputWriter(wg *sync.WaitGroup, input <-chan string) {
	defer (*wg).Done()
	w := bufio.NewWriter(os.Stdout)
	for line := range input {
		w.WriteString(line)
	}
	w.Flush()
}

func worker(wg *sync.WaitGroup, input <-chan string, output chan<- string) {
	defer (*wg).Done()
	client := redis.NewClient(&redis.Options{
		Addr:     redisServerUrl,
		Password: redisServerPass,
		DB:       redisServerDB,
	})

	openIPs := make(map[string]bool)
	outUrls := make(map[string]bool)

	for rawInput := range input {
		var ipAddr string
		if len(rawInput) > 0 {
			if rawInput[0] != '#' {
				ipAddr = rawInput
				openIPs[ipAddr] = true
			} else {
				ipAddr = rawInput[1:len(rawInput)]
				if _, ok := openIPs[ipAddr]; !ok {
					continue
				}
			}
		} else {
			continue
		}

		var domains []string
		var urls []string
		redisDomains, err := client.Get(ipAddr).Result()
		if err == redis.Nil { // no key found
			domains = make([]string, 0)
		} else if err != nil {
			log.Fatal("unable to get key:", err)
		} else {
			err = json.Unmarshal([]byte(redisDomains), &domains)
			if err != nil {
				log.Fatal("error unmarshalling redis string:", err)
			}
		}

		for _, domain := range domains {
			redisUrls, err := client.Get(domain).Result()
			if err == redis.Nil { // no key found
				urls = make([]string, 0)
			} else if err != nil {
				log.Fatal("unable to get key:", err)
			} else {
				err = json.Unmarshal([]byte(redisUrls), &urls)
				if err != nil {
					log.Fatal("error unmarshalling redis string:", err)
				}
			}

			for _, u := range urls {
				if hasSent, keyExist := outUrls[u]; keyExist && hasSent {
					continue
				}
				outUrls[u] = true
				output<-fmt.Sprintf("%s,%s\n", ipAddr, u)
			}

		}
	}
}

func main() {
	flags := flag.NewFlagSet("flags", flag.ExitOnError)
	flags.StringVar(&redisServerUrl, "redis-url", "127.0.0.1:6379", "URL for redis server that stores one-to-many IP:domain mapping")
	flags.StringVar(&redisServerPass, "redis-pass", "", "Password for redis server")
	flags.IntVar(&redisServerDB, "redis-db", 0, "DB for redis server")

	flags.Parse(os.Args[1:])

	outChan := make(chan string)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go outputWriter(&writerWG, outChan)


	var chans [256] chan string
	for i := range chans {
		chans[i] = make(chan string)
	}
	var WG sync.WaitGroup
	WG.Add(256)
	for i := 0; i < 256; i++ {
		go worker(&WG, chans[i], outChan);
	}

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		rawInput := s.Text()
		parts := strings.Split(rawInput, ".")
		if len(parts) == 0 {
			continue
		}
		idx,_ := strconv.Atoi(parts[len(parts)-1])
		chans[idx]<-rawInput
	}

	for i := range chans {
		close(chans[i])
	}
	WG.Wait()
	close(outChan)
	writerWG.Wait()

	if err := s.Err(); err != nil {
		log.Fatal("input unable to read stdin", err)
	}

}
