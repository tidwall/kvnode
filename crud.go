package roam

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

var errSyntaxError = errors.New("syntax error")

type dbValue struct {
	value   []byte
	usingEx bool
	ex      time.Duration
}

func (dbv *dbValue) Bytes() []byte {
	if dbv.usingEx {
		b := make([]byte, 9+len(dbv.value))
		b[0] = 1
		binary.LittleEndian.PutUint64(b[1:], uint64(dbv.ex))
		copy(b[9:], dbv.value)
		return b
	}
	b := make([]byte, 1+len(dbv.value))
	copy(b[1:], dbv.value)
	return b
}

func parseDBValue(b []byte, peek bool) (*dbValue, error) {
	var dbv dbValue
	if len(b) == 0 {
		return nil, errors.New("invalid value size")
	}
	switch b[0] {
	default:
		return nil, errors.New("invalid value type")
	case 0:
		if peek {
			dbv.value = b[1:]
		} else {
			dbv.value = make([]byte, len(b)-1)
			copy(dbv.value, b[1:])
		}
		return &dbv, nil
	case 1:
		if len(b) < 9 {
			return nil, errors.New("invalid value size")
		}
		dbv.usingEx = true
		dbv.ex = time.Duration(binary.LittleEndian.Uint64(b[1:]))
		if peek {
			dbv.value = b[9:]
		} else {
			dbv.value = make([]byte, len(b)-9)
			copy(dbv.value, b[9:])
		}
		return &dbv, nil
	}
}

func (kvm *Machine) cmdSet(
	m finn.Applier, conn redcon.Conn, cmd redcon.Command,
) (interface{}, error) {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var dbv dbValue
	dbv.value = cmd.Args[2]
	for i := 3; i < len(cmd.Args); i++ {
		switch strings.ToLower(string(cmd.Args[i])) {
		default:
			return nil, errSyntaxError
		case "ex":
			i++
			if i == len(cmd.Args) {
				return nil, errSyntaxError
			}
			n, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil {
				return nil, errSyntaxError
			}
			dbv.ex = time.Duration(n * float64(time.Second))
			dbv.usingEx = true
		}
	}
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			tx, err := kvm.db.OpenTransaction()
			if err != nil {
				return nil, err
			}
			defer tx.Discard()
			if err := kvm.db.Put(cmd.Args[1], dbv.Bytes(), kvm.wo); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, nil
		},
		func(v interface{}) (interface{}, error) {
			conn.WriteString("OK")
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdGet(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			val, err := kvm.db.Get(cmd.Args[1], nil)
			if err != nil {
				if err == leveldb.ErrNotFound {
					conn.WriteNull()
					return nil, nil
				}
				return nil, err
			}
			dbv, err := parseDBValue(val, true)
			if err != nil {
				return nil, err
			}
			conn.WriteBulk(dbv.value)
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdDel(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	deleted := make([][]byte, 0, len(cmd.Args))
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			tx, err := kvm.db.OpenTransaction()
			if err != nil {
				return 0, err
			}
			defer tx.Discard()
			var n int
			for i := 1; i < len(cmd.Args); i++ {
				has, err := tx.Has(cmd.Args[i], nil)
				if err != nil && err != leveldb.ErrNotFound {
					return 0, err
				} else if has {
					n++
					if err := tx.Delete(cmd.Args[i], kvm.wo); err != nil {
						return nil, err
					}
					deleted = append(deleted, cmd.Args[i])
				}
			}
			if err := tx.Commit(); err != nil {
				return 0, err
			}
			for _, key := range deleted {
				kvm.deleteExItem(key)
			}
			return n, nil
		},
		func(v interface{}) (interface{}, error) {
			n := v.(int)
			conn.WriteInt(n)
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdKeys(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var withvalues bool
	var pivot []byte
	var usingPivot bool
	var desc bool
	limit := 500
	for i := 2; i < len(cmd.Args); i++ {
		switch strings.ToLower(string(cmd.Args[i])) {
		default:
			return nil, errSyntaxError
		case "withvalues":
			withvalues = true
		case "desc":
			desc = true
		case "pivot":
			i++
			if i == len(cmd.Args) {
				return nil, errSyntaxError
			}
			pivot = cmd.Args[i]
			usingPivot = true
		case "limit":
			i++
			if i == len(cmd.Args) {
				return nil, errSyntaxError
			}
			n, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil || n < 0 {
				return nil, errSyntaxError
			}
			limit = int(n)
		}
	}
	pattern := cmd.Args[1]
	spattern := string(pattern)
	min, max := match.Allowable(spattern)
	bmin := []byte(min)
	bmax := []byte(max)
	var keys [][]byte
	var vals [][]byte
	useMax := !(len(spattern) > 0 && spattern[0] == '*')
	iter := kvm.db.NewIterator(nil, nil)
	var ok bool
	var movedPast bool
	if desc {
		var lasted bool
		if min == "" && max == "" && spattern != "" {
			if usingPivot {
				bmax = pivot
			} else {
				ok = iter.Last()
				lasted = true
			}
		} else if usingPivot {
			if bytes.Compare(pivot, bmax) < 0 {
				bmax = pivot
			}
		}
		if !lasted {
			ok = iter.Seek(bmax)
			if !ok {
				ok = iter.Last()
			} else {
				for ; ok; ok = iter.Prev() {
					key := iter.Key()
					if bytes.Compare(key, bmax) <= 0 {
						break
					}
				}
			}
		}
	} else {
		if usingPivot {
			if bytes.Compare(pivot, bmin) > 0 {
				bmin = pivot
			}
		}
		ok = iter.Seek(bmin)
	}
	step := func() bool {
		if desc {
			return iter.Prev()
		}
		return iter.Next()
	}
	for ok = iter.Valid(); ok; ok = step() {
		key := iter.Key()
		if usingPivot {
			if !movedPast {
				if !desc {
					if bytes.Compare(key, pivot) <= 0 {
						continue
					}
				} else {
					if bytes.Compare(key, pivot) >= 0 {
						continue
					}
				}
				movedPast = true
			}
		}
		if len(keys) >= limit {
			break
		}
		skey := string(key)
		if useMax && skey >= max {
			break
		}
		if match.Match(skey, spattern) {
			keys = append(keys, []byte(skey))
			if withvalues {
				value := iter.Value()
				vals = append(vals, bcopy(value))
			}
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		if withvalues {
			conn.WriteArray(len(keys) * 2)
		} else {
			conn.WriteArray(len(keys))
		}
		for i := 0; i < len(keys); i++ {
			conn.WriteBulk(keys[i])
			if withvalues {
				conn.WriteBulk(vals[i])
			}
		}
	}
	return nil, nil
}

func bcopy(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

func (kvm *Machine) cmdFlushdb(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if err := kvm.db.Close(); err != nil {
		panic(err.Error())
	}
	if err := os.RemoveAll(kvm.dbPath); err != nil {
		panic(err.Error())
	}
	var err error
	kvm.db, err = leveldb.OpenFile(kvm.dbPath, kvm.opts)
	if err != nil {
		panic(err.Error())
	}
	conn.WriteString("OK")
	return nil, nil
}
