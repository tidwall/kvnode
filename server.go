package roam

import (
	"os"

	"github.com/tidwall/finn"
	"github.com/tidwall/redlog"
)

var log = redlog.New(os.Stderr)

func ListenAndServe(addr, dir string) error {
	var opts finn.Options
	opts.Backend = finn.LevelDB
	opts.Consistency = finn.Low
	opts.Durability = finn.Low
	m, err := NewMachine(dir, opts.Durability == finn.High)
	if err != nil {
		return err
	}
	n, err := finn.Open(dir, addr, "", m, &opts)
	if err != nil {
		return err
	}
	defer n.Close()
	select {
	// blocking
	}
}
