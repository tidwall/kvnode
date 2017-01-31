package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tidwall/redlog"
	"github.com/tile38/kvnode"
)

func main() {
	var port int
	var data string
	var join string
	flag.IntVar(&port, "p", 4920, "server port")
	flag.StringVar(&data, "d", "data", "data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.Parse()
	var log = redlog.New(os.Stderr)
	addr := fmt.Sprintf(":%d", port)
	if err := node.ListenAndServe(addr, join, data); err != nil {
		log.Warningf("%v", err)
	}
}
