package common

import "sync"

type LockWithObj struct {
	obj  any
	lock sync.Mutex
}

func NewLockWithObj(obj any) *LockWithObj {
	return &LockWithObj{
		obj:  obj,
		lock: sync.Mutex{},
	}
}

func (l LockWithObj) Lock() {
	l.lock.Lock()
}
func (l LockWithObj) Unlock() {
	l.lock.Unlock()
}
