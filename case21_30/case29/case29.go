package case29

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Membership struct {
	ID       int64  `json:"id"`
	UserID   int64  `json:"user_id"`
	ExpireAt string `json:"expire_at"` // 假设到期时间是字符串格式
}

func InitDB(db *gorm.DB) {
	db.AutoMigrate(&Membership{})
}

type MembershipDao struct {
	db *gorm.DB
}

func NewMembershipDao(db *gorm.DB) *MembershipDao {
	return &MembershipDao{db: db}
}

func (o *MembershipDao) DeadLock() error {
	var eg errgroup.Group
	eg.Go(func() error {
		return o.AddMembership(context.Background(), &Membership{
			ID:       1,
			UserID:   1,
			ExpireAt: "2024-01-01", // 假设到期时间
		})
	})
	eg.Go(func() error {
		return o.AddMembership(context.Background(), &Membership{
			ID:       2,
			UserID:   2,
			ExpireAt: "2024-01-01",
		})
	})
	eg.Go(func() error {
		return o.AddMembership(context.Background(), &Membership{
			ID:       3,
			UserID:   3,
			ExpireAt: "2024-01-01",
		})
	})
	return eg.Wait()
}

func (o *MembershipDao) RepairDeadLock() error {
	var eg errgroup.Group
	eg.Go(func() error {
		return o.AddMembershipV1(context.Background(), &Membership{
			ID:       4,
			UserID:   4,
			ExpireAt: "2024-01-01",
		})
	})
	eg.Go(func() error {
		return o.AddMembershipV1(context.Background(), &Membership{
			ID:       5,
			UserID:   5,
			ExpireAt: "2024-01-01",
		})
	})
	eg.Go(func() error {
		return o.AddMembershipV1(context.Background(), &Membership{
			ID:       6,
			UserID:   6,
			ExpireAt: "2024-01-01",
		})
	})
	return eg.Wait()
}

func (o *MembershipDao) AddMembership(ctx context.Context, membership *Membership) error {
	tx := o.db.Begin()
	err := tx.WithContext(ctx).Model(membership).Where("user_id = ?", membership.UserID).Clauses(clause.Locking{Strength: "UPDATE"}).First(&membership).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		tx.Rollback()
		return err
	}
	if err == nil {
		// 会员已存在，更新到期时间
		err = tx.WithContext(ctx).Model(membership).Where("user_id = ?", membership.UserID).Update("expire_at", membership.ExpireAt).Error
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 会员不存在，创建新会员记录
		err = tx.WithContext(ctx).Create(membership).Error
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func (o *MembershipDao) AddMembershipV1(ctx context.Context, membership *Membership) error {
	// 使用 Create 方法结合 OnConflict 来实现 Upsert
	return o.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "user_id"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"expire_at"}),
	}).Create(membership).Error
}
