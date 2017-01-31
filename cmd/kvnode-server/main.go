package main

import (
	"flag"
	"os"

	"github.com/tidwall/redlog"
	"github.com/tile38/kvnode"
)

func main() {
	var addr string
	var data string
	var join string
	flag.StringVar(&addr, "addr", "127.0.0.1:4920", "bind/discoverable ip:port")
	flag.StringVar(&data, "data", "data", "data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.Parse()
	var log = redlog.New(os.Stderr)
	if err := node.ListenAndServe(addr, join, data); err != nil {
		log.Warningf("%v", err)
	}
}
