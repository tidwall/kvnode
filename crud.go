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
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
	"github.com/tile38/roam/finn"
)

// k{KEY} -> data
// e{KEY} -> unix-ttl + index
// x{UNIX}{INDEX}{KEY} -> empty

var errSyntaxError = errors.New("syntax error")

func now() time.Time {
	return time.Now()
}

func makeKey(prefix byte, b []byte) []byte {
	key := make([]byte, 1+len(b))
	key[0] = prefix
	copy(key[1:], b)
	return key
}

func fromKey(key []byte) []byte {
	return key[1:]
}

func (kvm *Machine) cmdSet(
	m finn.Applier, conn redcon.Conn, cmd redcon.Command,
	index uint64,
) (interface{}, error) {
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var unix uint64
	var usingEx bool
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
			if err != nil || n < 0 {
				return nil, errSyntaxError
			}
			unix = uint64(now().Add(time.Duration(n * float64(time.Second))).UnixNano())
			usingEx = true
		}
	}
	kKey := makeKey('k', cmd.Args[1])
	eKey := makeKey('e', cmd.Args[1])
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			kvm.mu.Lock()
			defer kvm.mu.Unlock()
			var batch leveldb.Batch
			eVal, err := kvm.db.Get(eKey, nil)
			if err != nil {
				if err != leveldb.ErrNotFound {
					return nil, err
				}
			} else {
				if len(eVal) != 16 {
					return nil, errors.New("invalid exkey value")
				}
				xKey := make([]byte, 17+len(cmd.Args[1]))
				xKey[0] = 'x'
				copy(xKey[1:], eVal)
				copy(xKey[17:], cmd.Args[1])
				batch.Delete(xKey)
				if !usingEx {
					batch.Delete(eKey)
				}
			}
			if usingEx {
				eVal := make([]byte, 16)
				binary.BigEndian.PutUint64(eVal, unix)
				binary.BigEndian.PutUint64(eVal[8:], index)
				batch.Put(eKey, eVal)
				xKey := make([]byte, 17+len(cmd.Args[1]))
				xKey[0] = 'x'
				copy(xKey[1:], eVal)
				copy(xKey[17:], cmd.Args[1])
				batch.Put(xKey, []byte{})
			}
			batch.Put(kKey, cmd.Args[2])
			return nil, kvm.db.Write(&batch, kvm.wo)
		},
		func(v interface{}) (interface{}, error) {
			conn.WriteString("OK")
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdDelif(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	maxidx, err := strconv.ParseUint(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			kvm.mu.Lock()
			defer kvm.mu.Unlock()
			var batch leveldb.Batch
			for _, key := range cmd.Args[2:] {
				eKey := makeKey('e', key)
				eVal, err := kvm.db.Get(eKey, nil)
				if err != nil || len(eVal) != 16 {
					continue // just ignore the error for now
				}
				index := binary.BigEndian.Uint64(eVal[8:])
				if index <= maxidx {
					kKey := makeKey('k', key)
					xKey := make([]byte, 17+len(key))
					xKey[0] = 'x'
					copy(xKey[1:], eVal)
					copy(xKey[17:], key)
					batch.Delete(eKey)
					batch.Delete(kKey)
					batch.Delete(xKey)
				}
			}
			return nil, kvm.db.Write(&batch, kvm.wo)
		},
		func(v interface{}) (interface{}, error) {
			conn.WriteString("OK")
			return nil, nil
		},
	)
}
func (kvm *Machine) cmdListex(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	limit, err := strconv.ParseUint(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	n := now()
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			kvm.mu.RLock()
			defer kvm.mu.RUnlock()
			s, err := kvm.db.GetSnapshot()
			if err != nil {
				return nil, err
			}
			defer s.Release()
			iter := s.NewIterator(nil, nil)
			var keys [][]byte
			var indexes []uint64
			ok := iter.Seek([]byte{'x'})
			for ; ok; ok = iter.Next() {
				if len(keys) == int(limit) {
					break
				}
				xKey := iter.Key()
				if len(xKey) < 17 || xKey[0] != 'x' {
					break
				}
				exi := int64(binary.BigEndian.Uint64(xKey[1:]))
				ex := time.Unix(0, exi)
				if ex.After(n) {
					break
				}
				index := uint64(binary.BigEndian.Uint64(xKey[9:]))
				keys = append(keys, bcopy(xKey[17:]))
				indexes = append(indexes, index)
			}
			iter.Release()
			err = iter.Error()
			if err != nil {
				return nil, err
			}
			conn.WriteArray(len(keys) * 2)
			for i := 0; i < len(keys); i++ {
				conn.WriteBulk(keys[i])
				conn.WriteBulkString(strconv.FormatUint(indexes[i], 10))
			}
			return nil, nil
		},
	)
}
func (kvm *Machine) cmdDump(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			kvm.mu.RLock()
			defer kvm.mu.RUnlock()
			s, err := kvm.db.GetSnapshot()
			if err != nil {
				return nil, err
			}
			defer s.Release()
			iter := s.NewIterator(nil, nil)
			var keys [][]byte
			var vals [][]byte
			for ok := iter.First(); ok; ok = iter.Next() {
				keys = append(keys, bcopy(iter.Key()))
				vals = append(vals, bcopy(iter.Value()))
			}
			iter.Release()
			err = iter.Error()
			if err != nil {
				return nil, err
			}
			conn.WriteArray(len(keys) * 2)
			for i := 0; i < len(keys); i++ {
				conn.WriteBulk(keys[i])
				conn.WriteBulk(vals[i])
			}
			return nil, nil
		},
	)
}
func (kvm *Machine) cmdGet(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			kvm.mu.RLock()
			defer kvm.mu.RUnlock()
			value, err := kvm.db.Get(makeKey('k', cmd.Args[1]), nil)
			if err != nil {
				if err == leveldb.ErrNotFound {
					conn.WriteNull()
					return nil, nil
				}
				return nil, err
			}
			conn.WriteBulk(value)
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdDel(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			kvm.mu.Lock()
			defer kvm.mu.Unlock()
			var batch leveldb.Batch
			var n int
			for i := 1; i < len(cmd.Args); i++ {
				kKey := makeKey('k', cmd.Args[i])
				has, err := kvm.db.Has(kKey, nil)
				if err != nil && err != leveldb.ErrNotFound {
					return 0, err
				} else if has {
					n++
					batch.Delete(kKey)
					eKey := makeKey('e', cmd.Args[i])
					eVal, err := kvm.db.Get(eKey, nil)
					if err == nil {
						if len(eVal) != 16 {
							return nil, errors.New("invalid exkey value")
						}
						xKey := make([]byte, 17+len(cmd.Args[1]))
						xKey[0] = 'x'
						copy(xKey[1:], eVal)
						copy(xKey[17:], cmd.Args[1])
						batch.Delete(xKey)
						batch.Delete(eKey)
					} else if err != leveldb.ErrNotFound {
						return nil, err
					}
				}
			}
			if err := kvm.db.Write(&batch, kvm.wo); err != nil {
				return nil, err
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
			pivot = makeKey('k', cmd.Args[i])
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
	pattern := makeKey('k', cmd.Args[1])
	spattern := string(pattern)
	min, max := match.Allowable(spattern)
	bmin := []byte(min)
	bmax := []byte(max)
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			kvm.mu.RLock()
			defer kvm.mu.RUnlock()
			var keys [][]byte
			var values [][]byte
			iter := kvm.db.NewIterator(nil, nil)
			var ok bool
			if desc {
				if usingPivot && bytes.Compare(pivot, bmax) < 0 {
					bmax = pivot
				}
				ok = iter.Seek(bmax)
				if !ok {
					ok = iter.Last()
				}
			} else {
				if usingPivot && bytes.Compare(pivot, bmin) > 0 {
					bmin = pivot
				}
				ok = iter.Seek(bmin)
			}
			step := func() bool {
				if desc {
					return iter.Prev()
				} else {
					return iter.Next()
				}
			}
			var inRange bool
			for ; ok; ok = step() {
				if len(keys) == limit {
					break
				}
				rkey := iter.Key()
				if desc {
					if !inRange {
						if bytes.Compare(rkey, bmax) >= 0 {
							continue
						}
						inRange = true
					}
					if bytes.Compare(rkey, bmin) < 0 {
						break
					}
				} else {
					if !inRange {
						if usingPivot {
							if bytes.Compare(rkey, bmin) <= 0 {
								continue
							}
						}
						inRange = true
					}
					if bytes.Compare(rkey, bmax) >= 0 {
						break
					}
				}
				skey := string(rkey)
				if !match.Match(skey, spattern) {
					continue
				}
				keys = append(keys, bcopy(rkey[1:]))
				if withvalues {
					values = append(values, bcopy(iter.Value()))
				}
			}
			iter.Release()
			err := iter.Error()
			if err != nil {
				return nil, err
			}
			if withvalues {
				conn.WriteArray(len(keys) * 2)
			} else {
				conn.WriteArray(len(keys))
			}
			for i := 0; i < len(keys); i++ {
				conn.WriteBulk(keys[i])
				if withvalues {
					conn.WriteBulk(values[i])
				}
			}
			return nil, nil
		},
	)
}

func bcopy(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

func (kvm *Machine) cmdFlushdb(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			kvm.mu.Lock()
			defer kvm.mu.Unlock()
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
			return nil, nil
		},
		func(v interface{}) (interface{}, error) {
			conn.WriteString("OK")
			return nil, nil
		},
	)
}
