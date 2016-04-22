package locks

import "sync"

var GlobalMetadataLock = &sync.Mutex{}
var GlobalWriteLock = &sync.Mutex{}
var GlobalCursorLock = &sync.Mutex{}
var allLocks = []*sync.Mutex{GlobalMetadataLock, GlobalWriteLock, GlobalCursorLock}

func StopTheWorld() {
	for _, lock := range allLocks {
		lock.Lock()
	}
}

func UnstopTheWorld() {
	for _, lock := range allLocks {
		lock.Unlock()
	}
}
