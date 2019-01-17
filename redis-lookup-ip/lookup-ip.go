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
	_ "../../zdns/iohandlers/file"
	_ "../../zdns/modules/alookup"
	_ "../../zdns/modules/axfr"
	_ "../../zdns/modules/dmarc"
	_ "../../zdns/modules/miekg"
	_ "../../zdns/modules/mxlookup"
	_ "../../zdns/modules/nslookup"
	_ "../../zdns/modules/spf"
	"bufio"
	"encoding/json"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"os"
	"strings"
)

var (
	redisServerUrl string
	redisServerPass string
	redisServerDB int
)

func main() {
	flags := flag.NewFlagSet("flags", flag.ExitOnError)
	flags.StringVar(&redisServerUrl, "redis-url", "127.0.0.1:6379", "URL for redis server that stores one-to-many IP:domain mapping")
	flags.StringVar(&redisServerPass, "redis-pass", "", "Password for redis server")
	flags.IntVar(&redisServerDB, "redis-db", 0, "DB for redis server")

	flags.Parse(os.Args[1:])

	client := redis.NewClient(&redis.Options{
		Addr:     redisServerUrl,
		Password: redisServerPass,
		DB:       redisServerDB,
	})


	s := bufio.NewScanner(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	for s.Scan() {
		ipAddr := s.Text()

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
				domains = make([]string, 0)
			} else if err != nil {
				log.Fatal("unable to get key:", err)
			} else {
				err = json.Unmarshal([]byte(redisUrls), &urls)
				if err != nil {
					log.Fatal("error unmarshalling redis string:", err)
				}
			}
		}

		for _, u := range urls {
			outputStr := []string{ipAddr, u + "\n"}
			w.WriteString(strings.Join(outputStr, ","))
		}
	}
	w.Flush()

	if err := s.Err(); err != nil {
		log.Fatal("input unable to read stdin", err)
	}
}

