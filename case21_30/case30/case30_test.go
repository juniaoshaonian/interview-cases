package case30

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"interview-cases/test"
	"testing"
	"time"
)

type User struct {
	ID   int64 `gorm:"primaryKey;autoIncrement"` // 设置为主键
	Name string
	// 积分
	Credit float64
	// 版本
	Version int32
}

type TestSuite struct {
	suite.Suite
	db *gorm.DB
}

func (t *TestSuite) SetupSuite() {
	t.db = test.InitDB()
	// 借助 GORM 来初始化表结构
	err := t.db.AutoMigrate(&User{})
	require.NoError(t.T(), err)
	// 初始化一些数据
	t.prepareData()
}

func (t *TestSuite) TestTableLock() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t.db.Transaction(func(tx *gorm.DB) error {
		// select for update 查找一个不存在列会导致锁表
		var us User
		err := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Model(&User{}).
			Where("id = ?", 20000).First(&us).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 稍微把时间调长一点方便观察表锁现象
				time.Sleep(30 * time.Second)
				return nil
			} else {
				return err
			}
		}
		if us.Credit > 100 {
			// 如果积分大于100,就扣100分
			tx.WithContext(ctx).Where("id = ?", 20000).
				Model(&User{}).
				Update("credit", gorm.Expr("credit - ?", 100))
			return nil
		} else {
			return errors.New("积分不足")
		}
	})
}

func (t *TestSuite) TestCas() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		var us User
		err := t.db.WithContext(ctx).
			Model(&User{}).
			Where("id = ? ", 1000).First(&us).Error
		if err != nil {
			return
		}
		// 找到就更新
		if us.Credit > 100 {
			res := t.db.WithContext(ctx).
				Model(&User{}).
				Where("id = ? and version = ? ", 1000, us.Version).
				Updates(map[string]any{
					"credit":  gorm.Expr("credit - ?", 100),
					"version": us.Version + 1,
				})
			if res.RowsAffected == 0 {
				// 更新失败，说明可能有并发操作，比如说用户开了好几个页面一起兑换礼物，想要找你的漏洞
				continue
			}
			// 更新成功
			return

		} else {
			// 不足100就返回
			return
		}
	}

}

func (t *TestSuite) prepareData() {
	// 先清空数据
	err := t.db.WithContext(context.Background()).Exec("DELETE FROM users;").Error
	require.NoError(t.T(), err)
	users := make([]*User, 0, 32)
	// 插入1000条数据
	for i := 1; i <= 1000; i++ {
		u := &User{
			ID:     int64(i),
			Name:   fmt.Sprintf("user_%d", i),
			Credit: 1000,
		}
		users = append(users, u)
	}
	err = t.db.WithContext(context.Background()).Create(users).Error
	require.NoError(t.T(), err)
}

func TestCase30(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
