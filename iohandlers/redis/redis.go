package redis

import (
	"encoding/json"
	"github.com/kwang40/zdns/modules/miekg"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"

	"../../../zdns"
	"github.com/go-redis/redis"
	"sync"
)

type RedisOutputHandler struct {
	client *redis.Client
}

func (h *RedisOutputHandler) Initialize(conf *zdns.GlobalConf) {
	h.client = redis.NewClient(&redis.Options{
		Addr:     conf.RedisServerUrl,
		Password: conf.RedisServerPass,
		DB:       conf.RedisServerDB,
	})
}

func (h *RedisOutputHandler) WriteResults(results <-chan zdns.Result, wg *sync.WaitGroup) error {
	defer (*wg).Done()

	for r := range results {
		res, ok := r.Data.(zdns.MiekgResult)
		if !ok {
			o, err := json.Marshal(res)
			if err != nil {
				log.Fatal(err)
			}
			log.Warn("unable to parse result: ", string(o))
		}

		for _, a := range res.Answers {
			var key, domain string
			if miekgAnswer, ok := a.(zdns.MiekgAnswer); ok {
				if miekgAnswer.Type == dns.TypeToString[dns.TypeA] || miekgAnswer.Type == dns.TypeToString[dns.TypeAAAA] {
					key = miekgAnswer.Answer
					domain = miekgAnswer.Name
				}
			} else if mxAnswer, ok := a.(miekg.MXAnswer); ok {
				key = mxAnswer.Answer.Answer
				domain = mxAnswer.Answer.Name
			} else {
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
