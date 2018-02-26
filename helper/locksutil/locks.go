package locksutil

import (
	"crypto/md5"
	"math"
	"sync"

	"github.com/hashicorp/vault/helper/strutil"
)

const (
	DefaultLockCount = 256

	// For sanity. This can be changed when needed.
	MaxLockCount = 2048
)

type LockEntry struct {
	sync.RWMutex
}

// CreateLocks returns an array so that the locks can be itterated over in
// order.
//
// This is only threadsafe if a process is using a single lock, or iterating
// over the entire lock slice in order. Using a consistant order avoids
// deadlocks because you can never have the following:
//
// Lock A, Lock B
// Lock B, Lock A
//
// Where process 1 is now deadlocked trying to lock B, and process 2 deadlocked trying to lock A
//
func CreateLocks(lockCount int) []*LockEntry {
	switch {
	case lockCount <= 0:
		lockCount = DefaultLockCount
	case lockCount > MaxLockCount:
		lockCount = MaxLockCount
	}

	ret := make([]*LockEntry, lockCount)
	for i := range ret {
		ret[i] = new(LockEntry)
	}
	return ret
}

func LockIndexForKey(lockCount int, key string) int64 {
	hf := md5.New()
	hf.Write([]byte(key))
	hashVal := hf.Sum(nil)

	lockIndex, err := strutil.BitMaskedIndex(hashVal, bitsNeeded(lockCount))
	if err != nil {
		panic(err)
	}
	return lockIndex
}

func LockForKey(locks []*LockEntry, key string) *LockEntry {
	return locks[LockIndexForKey(len(locks), key)]
}

func LocksForKeys(locks []*LockEntry, keys []string) []*LockEntry {
	lockIndexes := make(map[int64]struct{}, len(keys))
	for _, k := range keys {
		lockIndexes[LockIndexForKey(len(locks), k)] = struct{}{}
	}

	locksToReturn := make([]*LockEntry, 0, len(keys))
	for i, l := range locks {
		if _, ok := lockIndexes[int64(i)]; ok {
			locksToReturn = append(locksToReturn, l)
		}
	}

	return locksToReturn
}

func bitsNeeded(value int) int {
	return int(math.Ceil(math.Log2(float64(value))))
}
