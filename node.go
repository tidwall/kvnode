package roam

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/redcon"
	"github.com/tile38/roam/finn"
)

type Machine struct {
	mu     sync.RWMutex // for FlushDB
	dir    string
	db     *leveldb.DB
	wo     *opt.WriteOptions
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
	kvm.wo = &opt.WriteOptions{Sync: false}
	kvm.db, err = leveldb.OpenFile(kvm.dbPath, kvm.opts)
	if err != nil {
		return nil, err
	}
	go kvm.selfManage()
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
	m finn.Applier, conn redcon.Conn, cmd redcon.Command, index uint64,
) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, finn.ErrUnknownCommand
	case "set":
		return kvm.cmdSet(m, conn, cmd, index)
	case "get":
		return kvm.cmdGet(m, conn, cmd)
	case "del":
		return kvm.cmdDel(m, conn, cmd)
	case "keys":
		return kvm.cmdKeys(m, conn, cmd)
	case "flushdb":
		return kvm.cmdFlushdb(m, conn, cmd)
	case "dump":
		return kvm.cmdDump(m, conn, cmd)
	case "listex":
		// LISTEX limit
		return kvm.cmdListex(m, conn, cmd)
	case "delif":
		// DELIF maxindex key [key ...]
		return kvm.cmdDelif(m, conn, cmd)
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
			if err := kvm.db.Write(batch, kvm.wo); err != nil {
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
	if err := kvm.db.Write(batch, kvm.wo); err != nil {
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

func (kvm *Machine) selfManage() {
	var fast bool
	for {
		if fast {
			time.Sleep(time.Millisecond)
		} else {
			time.Sleep(time.Second / 4)
		}
		fast = false
		ok := func() bool {
			kvm.mu.RLock()
			if kvm.closed {
				kvm.mu.RUnlock()
				return false
			}
			kvm.mu.RUnlock()
			const K = 1000
			// ok let's find out if there're any keys that need deleting
			v, err := do(kvm.addr, "LISTEX", K)
			if err != nil {
				errs := err.Error()
				if errs != "ERR leader not known" &&
					!strings.HasPrefix(errs, "TRY ") {
					log.Warningf("%v", err)
				}
				return true
			}
			ss, ok := v.([]string)
			if !ok || len(ss) == 0 || len(ss)%2 != 0 {
				return true
			}
			var delargs []interface{}
			var maxidx uint64
			delargs = append(delargs, "DELIF", 0)
			for i := 0; i < len(ss); i += 2 {
				key := ss[i+0]
				index, err := strconv.ParseUint(ss[i+1], 10, 64)
				if err != nil {
					log.Warningf("%v", err)
					return true
				}
				if index > maxidx {
					maxidx = index
				}
				delargs = append(delargs, key)
			}
			delargs[1] = maxidx
			v, err = do(kvm.addr, delargs...)
			if err != nil {
				log.Warningf("delfi failed: %v", err)
				return true
			}
			if s, ok := v.(string); !ok || s != "OK" {
				log.Warningf("delfi failed: %v", v)
				return true
			}
			if len(ss)/2 >= K {
				fast = true
			}
			return true
		}()
		if !ok {
			return
		}
	}
}

func do(addr string, args ...interface{}) (interface{}, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var buf []byte
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		args := fmt.Sprintf("%v", arg)
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(args)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, args...)
		buf = append(buf, '\r', '\n')
	}
	if _, err := conn.Write(buf); err != nil {
		return nil, err
	}
	rd := bufio.NewReader(conn)
	c, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}
	l, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	s := strings.TrimSpace(l)
	if c == '-' {
		return nil, errors.New(s)
	} else if c == '+' {
		return s, nil
	} else if c == '$' {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		data := make([]byte, int(n)+2)
		if _, err := io.ReadFull(rd, data); err != nil {
			return nil, err
		}
		if data[len(data)-2] != '\r' || data[len(data)-1] != '\n' {
			return nil, errors.New("invalid line ending")
		}
		return string(data[:len(data)-2]), nil
	} else if c == '*' {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		var res []string
		for i := 0; i < int(n); i++ {
			c, err := rd.ReadByte()
			if err != nil {
				return nil, err
			}
			l, err := rd.ReadString('\n')
			if err != nil {
				return nil, err
			}
			if c != '$' {
				return nil, errors.New("invalid character")
			}
			s := strings.TrimSpace(l)
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return nil, err
			}
			data := make([]byte, int(n)+2)
			if _, err := io.ReadFull(rd, data); err != nil {
				return nil, err
			}
			if data[len(data)-2] != '\r' || data[len(data)-1] != '\n' {
				return nil, errors.New("invalid line ending")
			}
			res = append(res, string(data[:len(data)-2]))
		}
		return res, nil
	}
	return nil, nil
}
