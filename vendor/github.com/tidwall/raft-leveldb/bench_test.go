package raftleveldb

import (
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
)

func BenchmarkLevelDBStore_FirstIndex(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkLevelDBStore_LastIndex(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkLevelDBStore_GetLog(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkLevelDBStore_StoreLog(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkLevelDBStore_StoreLogs(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkLevelDBStore_DeleteRange(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkLevelDBStore_Set(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkLevelDBStore_Get(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkLevelDBStore_SetUint64(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkLevelDBStore_GetUint64(b *testing.B) {
	store := testLevelDBStoreHigh(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}
