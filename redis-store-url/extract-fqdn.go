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
	_ "github.com/kwang40/zdns/iohandlers/file"
	_ "github.com/kwang40/zdns/modules/alookup"
	_ "github.com/kwang40/zdns/modules/axfr"
	_ "github.com/kwang40/zdns/modules/dmarc"
	_ "github.com/kwang40/zdns/modules/miekg"
	_ "github.com/kwang40/zdns/modules/mxlookup"
	_ "github.com/kwang40/zdns/modules/nslookup"
	_ "github.com/kwang40/zdns/modules/spf"
	"bufio"
	"encoding/json"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"net/url"
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
		urlStr := s.Text()

		if !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://"){
			urlStr = "http://" + urlStr
		}

		u, err := url.Parse(urlStr)
		if err != nil {
			log.Fatal("invalid url: ", err)
		}

		fqdn := u.Hostname()

		var value []string
		redisValue, err := client.Get(fqdn).Result()
		if err == redis.Nil { // no key found
			value = make([]string, 0)
		} else if err != nil {
			log.Fatal("unable to get key:", err)
		} else {
			err = json.Unmarshal([]byte(redisValue), &value)
			if err != nil {
				log.Fatal("error unmarshalling redis string:", err)
			}
		}

		if !contains(value, urlStr) {
			value = append(value, urlStr)
		}

		jsonBytes, err := json.Marshal(value)
		if err != nil {
			log.Fatal("error marshalling redis:", err)
		}
		err = client.Set(fqdn, string(jsonBytes[:]), 0).Err()
		if err != nil {
			log.Fatal("unable to set redis key:", err)
		}
		w.WriteString(fqdn + "\n")
	}
	w.Flush()

	if err := s.Err(); err != nil {
		log.Fatal("input unable to read stdin", err)
	}
}

func contains(arr []string, str string) bool {
	for _, val := range arr {
		if str == val {
			return true
		}
	}
	return false
}