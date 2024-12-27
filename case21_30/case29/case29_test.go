package case29

import (
	"interview-cases/test"
	"testing"
)

func TestCase29_DeadLock(t *testing.T) {
	db := test.InitDB()
	InitDB(db)
	membershipDao := NewMembershipDao(db)
	// 触发死锁
	_ = membershipDao.DeadLock()
}

func TestCase29_RepairDeadLock(t *testing.T) {
	db := test.InitDB()
	InitDB(db)
	membershipDao := NewMembershipDao(db)
	_ = membershipDao.RepairDeadLock()
}
