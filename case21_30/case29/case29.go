package case29

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Order struct {
	ID     int64   `json:"id"`
	UserID int64   `json:"user_id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

func InitDB(db *gorm.DB) {
	db.AutoMigrate(&Order{})
}

type OrderDao struct {
	db *gorm.DB
}

func NewOrderDao(db *gorm.DB) *OrderDao {
	return &OrderDao{db: db}
}

func (o *OrderDao) DeadLock() error {
	var eg errgroup.Group
	eg.Go(func() error {
		return o.AddOrder(context.Background(), &Order{
			ID:     1,
			UserID: 1,
			Amount: 100.0,
			Status: "Pending",
		})
	})
	eg.Go(func() error {
		return o.AddOrder(context.Background(), &Order{
			ID:     2,
			UserID: 2,
			Amount: 150.0,
			Status: "Pending",
		})
	})
	eg.Go(func() error {
		return o.AddOrder(context.Background(), &Order{
			ID:     3,
			UserID: 3,
			Amount: 200.0,
			Status: "Pending",
		})
	})
	return eg.Wait()
}

func (o *OrderDao) RepairDeadLock() error {
	var eg errgroup.Group
	eg.Go(func() error {
		return o.AddOrderV1(context.Background(), &Order{
			ID:     4,
			UserID: 4,
			Amount: 100.0,
			Status: "Pending",
		})
	})
	eg.Go(func() error {
		return o.AddOrderV1(context.Background(), &Order{
			ID:     4,
			UserID: 5,
			Amount: 150.0,
			Status: "Pending",
		})
	})
	eg.Go(func() error {
		return o.AddOrderV1(context.Background(), &Order{
			ID:     6,
			UserID: 6,
			Amount: 200.0,
			Status: "Pending",
		})
	})
	return eg.Wait()
}

func (o *OrderDao) AddOrder(ctx context.Context, order *Order) error {
	tx := o.db.Begin()
	err := tx.WithContext(ctx).Model(order).Where("id = ?", order.ID).Clauses(clause.Locking{Strength: "UPDATE"}).First(&order).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		tx.Rollback()
		return err
	}
	if err == nil {
		// 订单已存在，更新状态
		err = tx.WithContext(ctx).Model(order).Where("id = ?", order.ID).Update("status", order.Status).Error
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 订单不存在，创建新订单
		err = tx.WithContext(ctx).Create(order).Error
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func (o *OrderDao) AddOrderV1(ctx context.Context, order *Order) error {
	// 使用 Create 方法结合 OnConflict 来实现 Upsert
	return o.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "id"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"status"}),
	}).Create(order).Error
}
