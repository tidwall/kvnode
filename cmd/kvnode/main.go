package main

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
	"github.com/tidwall/sds"
	"github.com/tidwall/uhaha"
)

var db *leveldb.DB

func main() {
	var conf uhaha.Config
	conf.Name = "kvnode"
	conf.Version = "0.1.0"
	conf.DataDirReady = func(dir string) {
		os.RemoveAll(filepath.Join(dir, "main.db"))
		var err error
		db, err = leveldb.OpenFile(filepath.Join(dir, "main.db"),
			&opt.Options{NoSync: true})
		if err != nil {
			panic(err)
		}
	}
	conf.Snapshot = snapshot
	conf.Restore = restore
	conf.AddReadCommand("GET", cmdGET)
	conf.AddReadCommand("MGET", cmdMGET)
	conf.AddReadCommand("KEYS", cmdKEYS)
	conf.AddWriteCommand("SET", cmdSET)
	conf.AddWriteCommand("DEL", cmdDEL)
	conf.AddWriteCommand("MSET", cmdMSET)
	conf.AddWriteCommand("PDEL", cmdPDEL)
	uhaha.Main(conf)
}

func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	if err := db.Put([]byte(args[1]), []byte(args[2]), nil); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	val, err := db.Get([]byte(args[1]), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	var n int
	for i := 1; i < len(args); i++ {
		ok, err := db.Has([]byte(args[i]), nil)
		if err != nil {
			return nil, err
		}
		if ok {
			err := db.Delete([]byte(args[i]), nil)
			if err != nil {
				return nil, err
			}
			n++
		}
	}
	return redcon.SimpleInt(n), nil
}

func cmdPDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	pattern := args[1]
	min, max := match.Allowable(pattern)
	var keys []string
	iter := db.NewIterator(nil, nil)
	for ok := iter.Seek([]byte(min)); ok; ok = iter.Next() {
		key := string(iter.Key())
		if pattern != "*" {
			if key >= max {
				break
			}
			if !match.Match(key, pattern) {
				continue
			}
		}
		keys = append(keys, key)
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	var batch leveldb.Batch
	for _, key := range keys {
		batch.Delete([]byte(key))
	}
	if err := db.Write(&batch, nil); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdMSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, uhaha.ErrWrongNumArgs
	}
	var batch leveldb.Batch
	for i := 1; i < len(args); i += 2 {
		batch.Put([]byte(args[i]), []byte(args[i+1]))
	}
	if err := db.Write(&batch, nil); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdMGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	var vals []interface{}
	for i := 1; i < len(args); i++ {
		val, err := db.Get([]byte(args[i]), nil)
		if err != nil {
			if err == leveldb.ErrNotFound {
				vals = append(vals, nil)
				continue
			}
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

func cmdKEYS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	var withvalues bool
	var pivot string
	var usingPivot bool
	var desc bool
	var excl bool
	limit := math.MaxUint32
	for i := 2; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		default:
			return nil, uhaha.ErrSyntax
		case "withvalues":
			withvalues = true
		case "excl":
			excl = true
		case "desc":
			desc = true
		case "pivot":
			i++
			if i == len(args) {
				return nil, uhaha.ErrSyntax
			}
			pivot = args[i]
			usingPivot = true
		case "limit":
			i++
			if i == len(args) {
				return nil, uhaha.ErrSyntax
			}
			n, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || n < 0 {
				return nil, uhaha.ErrSyntax
			}
			limit = int(n)
		}
	}
	var min, max string

	pattern := args[1]
	var all bool
	if pattern == "*" {
		all = true
	} else {
		min, max = match.Allowable(pattern)
	}
	var ok bool
	var keys []string
	var values []string
	iter := db.NewIterator(nil, nil)
	step := func() bool {
		if desc {
			return iter.Prev()
		}
		return iter.Next()
	}
	if usingPivot {
		ok = iter.Seek([]byte(pivot))
		if ok && excl {
			key := string(iter.Key())
			if key == pivot {
				ok = step()
			}
		}
	} else {
		if all {
			if desc {
				ok = iter.Last()
			} else {
				ok = iter.First()
			}
		} else {
			if desc {
				ok = iter.Seek([]byte(max))
			} else {
				ok = iter.Seek([]byte(min))
			}
		}
	}
	for ; ok; ok = step() {
		if len(keys) == limit {
			break
		}
		key := string(iter.Key())
		if !all {
			if desc {
				if key < min {
					break
				}
			} else {
				if key > max {
					break
				}
			}
			if !match.Match(key, pattern) {
				continue
			}
		}
		keys = append(keys, key)
		if withvalues {
			values = append(values, string(iter.Value()))
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	var res []string
	if withvalues {
		for i := 0; i < len(keys); i++ {
			res = append(res, keys[i], values[i])
		}
	} else {
		for i := 0; i < len(keys); i++ {
			res = append(res, keys[i])
		}
	}
	return res, nil
}

type snap struct {
	s *leveldb.Snapshot
}

func (s *snap) Done(path string) {}
func (s *snap) Persist(wr io.Writer) error {
	sw := sds.NewWriter(wr)
	iter := s.s.NewIterator(nil, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := sw.WriteBytes(iter.Key()); err != nil {
			return err
		}
		if err := sw.WriteBytes(iter.Value()); err != nil {
			return err
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}
	return sw.Flush()
}

func snapshot(data interface{}) (uhaha.Snapshot, error) {
	s, err := db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &snap{s: s}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	sr := sds.NewReader(rd)
	var batch leveldb.Batch
	for {
		key, err := sr.ReadBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		value, err := sr.ReadBytes()
		if err != nil {
			return nil, err
		}
		batch.Put(key, value)
		if batch.Len() == 1000 {
			if err := db.Write(&batch, nil); err != nil {
				return nil, err
			}
			batch.Reset()
		}
	}
	if err := db.Write(&batch, nil); err != nil {
		return nil, err
	}
	return nil, nil
}
