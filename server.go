package node

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
)

var (
	errSyntaxError = errors.New("syntax error")
	log            = redlog.New(os.Stderr)
)

func ListenAndServe(addr, join, dir string, consistency, durability finn.Level) error {
	var opts finn.Options
	opts.Backend = finn.FastLog
	opts.Consistency = consistency
	opts.Durability = durability
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
	// blocking, there's no way out
	}
}

type Machine struct {
	mu     sync.RWMutex
	dir    string
	db     *leveldb.DB
	opts   *opt.Options
	dbPath string
	addr   string
	closed bool
}

func NewMachine(dir, addr string) (*Machine, error) {
	kvm := &Machine{
		dir:  dir,
		addr: addr,
	}
	var err error
	kvm.dbPath = filepath.Join(dir, "node.db")
	kvm.opts = &opt.Options{
		NoSync: true,
		Filter: filter.NewBloomFilter(10),
	}
	kvm.db, err = leveldb.OpenFile(kvm.dbPath, kvm.opts)
	if err != nil {
		return nil, err
	}
	return kvm, nil
}

func (kvm *Machine) Close() error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	kvm.db.Close()
	kvm.closed = true
	return nil
}

func (kvm *Machine) Command(
	m finn.Applier, conn redcon.Conn, cmd redcon.Command,
) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, finn.ErrUnknownCommand
	case "set":
		return kvm.cmdSet(m, conn, cmd)
	case "get":
		return kvm.cmdGet(m, conn, cmd)
	case "del":
		return kvm.cmdDel(m, conn, cmd)
	case "keys":
		return kvm.cmdKeys(m, conn, cmd)
	case "flushdb":
		return kvm.cmdFlushdb(m, conn, cmd)
	case "shutdown":
		log.Warningf("shutting down")
		conn.WriteString("OK")
		conn.Close()
		os.Exit(0)
		return nil, nil
	}
}

func (kvm *Machine) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	var err error
	if err := kvm.db.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(kvm.dbPath); err != nil {
		return err
	}
	kvm.db = nil
	kvm.db, err = leveldb.OpenFile(kvm.dbPath, kvm.opts)
	if err != nil {
		return err
	}
	var read int
	batch := new(leveldb.Batch)
	num := make([]byte, 8)
	gzr, err := gzip.NewReader(rd)
	if err != nil {
		return err
	}
	r := bufio.NewReader(gzr)
	for {
		if read > 4*1024*1024 {
			if err := kvm.db.Write(batch, nil); err != nil {
				return err
			}
			read = 0
		}
		if _, err := io.ReadFull(r, num); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, num); err != nil {
			return err
		}
		value := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}
		batch.Put(key, value)
		read += (len(key) + len(value))
	}
	if err := kvm.db.Write(batch, nil); err != nil {
		return err
	}
	return gzr.Close()
}

func (kvm *Machine) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	gzw := gzip.NewWriter(wr)
	ss, err := kvm.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer ss.Release()
	iter := ss.NewIterator(nil, nil)
	defer iter.Release()
	var buf []byte
	num := make([]byte, 8)
	for ok := iter.First(); ok; ok = iter.Next() {
		buf = buf[:0]
		key := iter.Key()
		value := iter.Value()
		binary.LittleEndian.PutUint64(num, uint64(len(key)))
		buf = append(buf, num...)
		buf = append(buf, key...)
		binary.LittleEndian.PutUint64(num, uint64(len(value)))
		buf = append(buf, num...)
		buf = append(buf, value...)
		if _, err := gzw.Write(buf); err != nil {
			return err
		}
	}
	if err := gzw.Close(); err != nil {
		return err
	}
	iter.Release()
	return iter.Error()
}

func (kvm *Machine) cmdSet(
	m finn.Applier, conn redcon.Conn, cmd redcon.Command,
) (interface{}, error) {
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.Apply(conn, cmd,
		func() (interface{}, error) {
			kvm.mu.Lock()
			defer kvm.mu.Unlock()
			return nil, kvm.db.Put(makeKey('k', cmd.Args[1]), cmd.Args[2], nil)
		},
		func(v interface{}) (interface{}, error) {
			conn.WriteString("OK")
			return nil, nil
		},
	)
}

func (kvm *Machine) cmdGet(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := makeKey('k', cmd.Args[1])
	return m.Apply(conn, cmd, nil,
		func(interface{}) (interface{}, error) {
			kvm.mu.RLock()
			defer kvm.mu.RUnlock()
			value, err := kvm.db.Get(key, nil)
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
				key := makeKey('k', cmd.Args[i])
				has, err := kvm.db.Has(key, nil)
				if err != nil && err != leveldb.ErrNotFound {
					return 0, err
				} else if has {
					n++
					batch.Delete(key)
				}
			}
			if err := kvm.db.Write(&batch, nil); err != nil {
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
func makeKey(prefix byte, b []byte) []byte {
	key := make([]byte, 1+len(b))
	key[0] = prefix
	copy(key[1:], b)
	return key
}

func bcopy(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
