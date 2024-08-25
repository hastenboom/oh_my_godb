package data_manager

import (
	"github.com/alphadose/haxmap"
	"sync"
)

type Long = int64
type Int = int32

/*type Func[T any] func(key Long) T

type CustomCache[T any] interface {
	//GetForCache get resource if cache miss
	GetForCache(key Long) T
	//ReleaseForCache write back resource when cache eviction
	ReleaseForCache(obj T)
}*/

/*
AbstractCache

# Long  T   Int    bool

id  data count isHandled
*/
type AbstractCache[T any] struct {
	Cache      *haxmap.Map[Long, T]   //Thread-safe, Cache is the actual cache
	References *haxmap.Map[Long, Int] // Thread-safe, using the COUNTER, rather than LRU, to track the number of references to each object in the cache
	//Getting     map[Long]bool
	MaxResource    Int
	Count          Int         //the number of objects in the cache
	CacheLock      sync.Locker // lock the whole AbstractCache
	CacheLockGroup []sync.Locker
}

func NewAbstractCache[T any](maxResource Int) *AbstractCache[T] {
	return &AbstractCache[T]{
		Cache:      haxmap.New[Long, T](),
		References: haxmap.New[Long, Int](),
		//Getting:     make(map[Long]bool),
		MaxResource:    maxResource,
		Count:          Int(0),
		CacheLock:      &sync.Mutex{},
		CacheLockGroup: []sync.Locker{},
	}
}

/*func (cache *AbstractCache[T]) Get2(key Long) T {
	return key
}*/

//func (cache *AbstractCache[T]) get(key Long) T {
//	//cacheRecord, cacheExist := cache.Cache[key]
//	cacheRecord, cacheExist := cache.Cache.Get(key)
//	//DCL
//	// as it doesn't exist, multi goroutine want to build that cache
//	if !cacheExist {
//
//		if cache.MaxResource > 0 && cache.Count >= cache.MaxResource {
//			panic(common.CACHE_FULL_ERROR)
//		}
//		func() {
//			//capacity is sufficient, go1 or go2 acquires the lock
//			//TODO:efficiency problem exists as I lock the whole cache
//			//cache.CacheLock.Lock()
//			//defer cache.CacheLock.Unlock()
//
//			common.NewLockWithObj(key)
//
//			_, cacheExist = cache.Cache[key]
//			if !cacheExist {
//				cacheRecord = cache.GetForCache(key)
//				cache.Count += 1
//			}
//		}()
//		// before go1 and go2 want to rebuild the cache, check the capacity of the cache
//	}
//
//	/*handle the reference count*/
//	//count++
//
//	return cacheRecord
//}

/*-------abstract methods-----------*/
func (cache AbstractCache[T]) GetForCache(key Long) T {
	panic("child should implement this method")
}

func (cache AbstractCache[T]) ReleaseForCache(obj T) {
	panic("child should implement this method")
}
