package trainmapdb

import (
	"sync"

	"gorm.io/gorm"
)

// this is a wrapper around the DB so it can be either mutexed (in which case there's an actual mutex) or not (in which case only the waitgroup is implemented).
// this is terrible, but SQLite had me do some abominations (no concurrency ;-;)
type syncCompatibleDB interface {
	getDB() *gorm.DB
	takeMutex()
	freeMutex()
	wgIncrement()
	wgDone()
	wgWait()
}

type unmutexedDB struct {
	db *gorm.DB
	wg sync.WaitGroup
}

func (u *unmutexedDB) getDB() *gorm.DB {
	return u.db
}
func (u *unmutexedDB) takeMutex() {}
func (u *unmutexedDB) freeMutex() {}
func (u *unmutexedDB) wgIncrement() {
	u.wg.Add(1)
}
func (u *unmutexedDB) wgDone() {
	u.wg.Done()
}
func (u *unmutexedDB) wgWait() {
	u.wg.Wait()
}

// a mutexedDB is just as ugly and horrible as it sounds (SQLite has forced my hand)
type mutexedDB struct {
	db    *gorm.DB
	mutex sync.Mutex
	wg    sync.WaitGroup
}

func (m *mutexedDB) getDB() *gorm.DB {
	return m.db
}
func (m *mutexedDB) takeMutex() {
	m.mutex.Lock()
}
func (m *mutexedDB) freeMutex() {
	m.mutex.Unlock()
}
func (m *mutexedDB) wgIncrement() {
	m.wg.Add(1)
}
func (m *mutexedDB) wgDone() {
	m.wg.Done()
}
func (m *mutexedDB) wgWait() {
	m.wg.Wait()
}
