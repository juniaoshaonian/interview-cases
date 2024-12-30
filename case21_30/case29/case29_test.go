package case29

import (
	"interview-cases/test"
	"testing"
)

func TestCase29_DeadLock(t *testing.T) {
	db := test.InitDB()
	InitDB(db)
	orderDao := NewOrderDao(db)
	// 触发死锁
	_ = orderDao.DeadLock()
}

func TestCase29_RepairDeadLock(t *testing.T) {
	db := test.InitDB()
	InitDB(db)
	orderDao := NewOrderDao(db)
	// 没有死锁的
	_ = orderDao.RepairDeadLock()
}

