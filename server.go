package roam

import (
	"os"

	"github.com/tidwall/redlog"
	"github.com/tile38/roam/finn"
)

var log = redlog.New(os.Stderr)

func ListenAndServe(addr, join, dir string) error {
	var opts finn.Options
	opts.Backend = finn.FastLog
	opts.Consistency = finn.High
	opts.Durability = finn.High
	m, err := NewMachine(dir, addr)
	if err != nil {
		return err
	}
	n, err := finn.Open(dir, addr, join, m, &opts)
	if err != nil {
		return err
	}
	defer n.Close()

	select {
	// blocking
	}
}
