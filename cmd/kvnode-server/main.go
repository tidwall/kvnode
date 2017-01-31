package main

import (
	"flag"
	"os"
	"strings"

	"github.com/tidwall/finn"
	"github.com/tidwall/redlog"
	"github.com/tile38/kvnode"
)

func main() {
	var addr string
	var data string
	var join string
	var consistency string
	var durability string
	flag.StringVar(&addr, "addr", "127.0.0.1:4920", "bind/discoverable ip:port")
	flag.StringVar(&data, "data", "data", "data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.StringVar(&consistency, "consistency", "high", "Consistency (low,medium,high)")
	flag.StringVar(&durability, "durability", "high", "Durability (low,medium,high)")
	flag.Parse()
	var log = redlog.New(os.Stderr)
	var lconsistency finn.Level
	switch strings.ToLower(consistency) {
	default:
		log.Warningf("invalid --consistency")
	case "low":
		lconsistency = finn.Low
	case "medium", "med":
		lconsistency = finn.Medium
	case "high":
		lconsistency = finn.High
	}
	var ldurability finn.Level
	switch strings.ToLower(durability) {
	default:
		log.Warningf("invalid --durability")
	case "low":
		ldurability = finn.Low
	case "medium", "med":
		ldurability = finn.Medium
	case "high":
		ldurability = finn.High
	}
	if err := node.ListenAndServe(addr, join, data, lconsistency, ldurability); err != nil {
		log.Warningf("%v", err)
	}
}
