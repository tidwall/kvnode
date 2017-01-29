package roam

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/btree"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

type exItem struct {
	key  []byte
	when time.Time
}

func (a *exItem) Less(v btree.Item, _ interface{}) bool {
	return a.when.Before(v.(*exItem).when)
}
func (kvm *Machine) putExItem(dbv *dbValue) {
}

func (kvm *Machine) deleteExItem(key []byte) {
}

type Machine struct {
	mu     sync.RWMutex // for FlushDB
	dir    string
	db     *leveldb.DB
	wo     *opt.WriteOptions
	opts   *opt.Options
	dbPath string
	extr   *btree.BTree
	exmp   map[string]time.Time
}

func NewMachine(dir string, sync bool) (*Machine, error) {
	kvm := &Machine{
		dir:  dir,
		extr: btree.New(32, nil),
		exmp: make(map[string]time.Time),
	}
	var err error
	kvm.dbPath = filepath.Join(dir, "node.db")
	kvm.opts = &opt.Options{
		NoSync: !sync,
		Filter: filter.NewBloomFilter(10),
	}
	kvm.wo = &opt.WriteOptions{Sync: sync}
	kvm.db, err = leveldb.OpenFile(kvm.dbPath, kvm.opts)
	if err != nil {
		return nil, err
	}
	return kvm, nil
}

func (kvm *Machine) Close() error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	kvm.db.Close()
	return nil
}

func (kvm *Machine) Command(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
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
		if read > 50*1024*1024 {
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
