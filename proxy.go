package main

import (
	"github.com/simonz05/godis/redis"
	"log"
	"net/http"
	"net/http/httputil"
)

const (
	NORMAL = iota
	MIGRATION
	CLEANUP
)

var (
	DefaultHost    = "localhost:3456"
	OldHost        = "localhost:3457"
	RunningDefault = true
)

func main() {
	director := func(req *http.Request) {
		req.URL.Scheme = "http"
		switch {
		case RunningDefault:
			req.URL.Host = DefaultHost
		case LegacyPort(req):
			req.URL.Host = OldHost
		default:
			req.URL.Host = DefaultHost
		}
	}
	proxy := &httputil.ReverseProxy{Director: director}
	go RedisChecker()
	log.Fatal(http.ListenAndServe(":5799", proxy))
}

func LegacyPort(req *http.Request) bool {
	return false
}

func RedisChecker() {
	ctx := ConnectToRedis()
	ctx.SetupWatcher()
	ctx.Watch()
}

func ConnectToRedis() *RedisContext {
	rc := new(RedisContext)
	rc.Conn = redis.New("tcp:127.0.0.1:6379", 0, "")
	status_type, err := rc.Conn.Type("migration_status")
	if err != nil {
		switch status_type {
		case "string":
			status, err := rc.Conn.Get("migration_status")
			if err == nil {
				rc.State = DecodeStatus(status.String())
			} else {
				log.Println("Could not get migration_status value, assuming normal")
				rc.State = NORMAL
			}
		default:
			log.Printf("Could not ascertain migration_status type (unknown type %s), assuming Normal", status_type)
		}
	} else {
		log.Println("Could not ascertain migration_status type due to error, assuming Normal")
		rc.State = NORMAL
	}

	return rc
}

type RedisContext struct {
	Conn  *redis.Client
	Sub   *redis.Sub
	State int
}

func (rc *RedisContext) SetupWatcher() {
	sub, err := rc.Conn.Subscribe("migrations")
	if err == nil {
		rc.Sub = sub
	} else {
		log.Fatal("Redis could not subscribe, is Redis started?")
	}
}

func (rc *RedisContext) Watch() {
	var change *redis.Message
	select {
	case change = <-rc.Sub.Messages:
		switch rc.State {
		case NORMAL:
			switch DecodeStatus(change.Elem.String()) {
			case MIGRATION:
				RunningDefault = false
				rc.State = MIGRATION
			case CLEANUP:
				RunningDefault = true
				rc.State = CLEANUP
			}
		case MIGRATION:
			switch DecodeStatus(change.Elem.String()) {
			case NORMAL:
				RunningDefault = true
				rc.State = NORMAL
			case CLEANUP:
				RunningDefault = true
				rc.State = CLEANUP
			}
		case CLEANUP:
			switch DecodeStatus(change.Elem.String()) {
			case MIGRATION:
				RunningDefault = false
				rc.State = MIGRATION
			case NORMAL:
				RunningDefault = true
				rc.State = NORMAL
			}
		}
	}
}

func DecodeStatus(status string) int {
	switch status {
	case "normal":
		return NORMAL
	case "migration":
		return MIGRATION
	case "cleanup":
		return CLEANUP
	}
	return NORMAL
}
