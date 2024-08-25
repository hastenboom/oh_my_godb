package tx

import (
	"errors"
	"fmt"
	fm "oh_my_godb/file_manager"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 3 //TODO: replace me with 10sec in prod
)

/*
LockTable

在java中，可直接使用JUC中的RWLock来替换，而不需要自己去维护下面这三个状态

基本逻辑如下，针对于每个block，维护3个东西
- lockMap: 锁的状态，-1表示XLock，>0表示SLock，0表示无锁
- notifyChan: 等待锁的channel，这个其实就是一个阻塞队列
- notifyWg: 等待锁的等待组

此时退化成如何设计单个RWLock

	R   W

R ⭕ ❌
W ❌ ❌
1. 对于R锁，检查是否有W锁，如果有，则阻塞等待notifyChan
2. 对于W锁，则要同时监测两个部分，
*/
type LockTable struct {
	// blockId->lockVal, -1 indicates XLock, >0 indicates SLock, 0 indicates no lock
	lockMap    map[*fm.BlockId]int64
	notifyChan map[*fm.BlockId]chan struct{}
	notifyWg   map[*fm.BlockId]*sync.WaitGroup
	methodLock *sync.Mutex
	RWLock     *sync.RWMutex
}

func NewLockTable() *LockTable {
	return &LockTable{
		lockMap:    make(map[*fm.BlockId]int64),
		notifyChan: make(map[*fm.BlockId]chan struct{}),
		notifyWg:   make(map[*fm.BlockId]*sync.WaitGroup),
	}
}

func (l *LockTable) SLock(blk *fm.BlockId) error {
	l.methodLock.Lock()
	defer l.methodLock.Unlock()

	l.initWaitingOnBlk(blk)

	start := time.Now()
	for l.hasXLock(blk) && !l.waitTooLong(start) {
		//l.waitGivenTimeOut(blk)

		func(blk *fm.BlockId) {
			wg, exist := l.notifyWg[blk]
			if !exist {
				var newWg sync.WaitGroup
				l.notifyWg[blk] = &newWg
				wg = &newWg
			}

			wg.Add(1)
			defer wg.Done()

			l.methodLock.Unlock()

			select {
			case <-time.After(MAX_WAITING_TIME * time.Second):
				fmt.Println("routine wake up for timeout")
			case <-l.notifyChan[blk]:
				fmt.Println("routine wake up for notify channel")
			}

			l.methodLock.Lock()
		}(blk)
	}

	if l.hasXLock(blk) {
		fmt.Println("SLock() fails to XLock()")
		return errors.New("SLock() fails to XLock()")
	}

	val := l.getLockVal(blk) //counter
	l.lockMap[blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockId) error {
	l.methodLock.Lock()
	defer l.methodLock.Unlock()

	l.initWaitingOnBlk(blk)

	start := time.Now()

	for l.hasOtherSLocks(blk) && !l.waitTooLong(start) {
		l.waitGivenTimeOut(blk)
	}

	if l.hasOtherSLocks(blk) {
		return errors.New("XLock() fails to SLock()")
	}

	l.lockMap[blk] = -1 // -1 indicates the mutex

	return nil
}

func (l *LockTable) UnLock(blk *fm.BlockId) {
	l.methodLock.Lock()
	defer l.methodLock.Unlock()

	val := l.getLockVal(blk)

	if val >= 1 {
		l.lockMap[blk] = val - 1
	} else {
		l.lockMap[blk] = 0
		l.notifyAll(blk)
	}

}

func (l *LockTable) initWaitingOnBlk(blk *fm.BlockId) {

	_, ok := l.notifyChan[blk]
	if !ok {
		l.notifyChan[blk] = make(chan struct{})
	}

	_, ok = l.notifyWg[blk]
	if !ok {
		l.notifyWg[blk] = &sync.WaitGroup{}
	}
}

/*
!key
*/
func (l *LockTable) waitGivenTimeOut(blk *fm.BlockId) {
	wg, exist := l.notifyWg[blk]
	if !exist {
		var newWg sync.WaitGroup
		l.notifyWg[blk] = &newWg
		wg = &newWg
	}

	wg.Add(1)
	defer wg.Done()

	l.methodLock.Unlock()
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		fmt.Println("routine wake up for timeout")
	case <-l.notifyChan[blk]:
		fmt.Println("routine wake up for notify channel")
	}

	l.methodLock.Lock()
}

/*
!key
*/
func (l *LockTable) notifyAll(blk *fm.BlockId) {

	go func() {
		l.notifyWg[blk].Wait()
		l.notifyChan[blk] = make(chan struct{})
	}()

	close(l.notifyChan[blk])
}

func (l *LockTable) hasXLock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) < 0
}

func (l *LockTable) hasOtherSLocks(blk *fm.BlockId) bool {
	return l.getLockVal(blk) > 0
}

func (l *LockTable) waitTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()

	if elapsed > MAX_WAITING_TIME {
		return true
	}

	return false
}

func (l *LockTable) getLockVal(blk *fm.BlockId) int64 {
	val, ok := l.lockMap[blk]
	if !ok {
		l.lockMap[blk] = 0
		return 0
	}
	return val
}
