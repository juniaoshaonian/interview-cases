package case32

import (
	"context"
	"github.com/gin-gonic/gin"
	"interview-cases/case31_40/case32/limiter"
	"interview-cases/case31_40/case32/monitor"
	"io/ioutil"
	"math/rand/v2"
	"net/http"
	"time"
)

// RateLimitBuilder 限流中间件
type RateLimitBuilder struct {
	mon   *monitor.RateLimitMon
	limit limiter.Limiter
}

func NewRateLimitBuilder(mon *monitor.RateLimitMon, limit limiter.Limiter) *RateLimitBuilder {
	return &RateLimitBuilder{
		mon:   mon,
		limit: limit,
	}
}
func CtxWithVip(ctx context.Context) context.Context {
	return context.WithValue(ctx, limiter.VipCtxKey, 1)
}
func (r *RateLimitBuilder) Build() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 记录活跃的请求数
		r.mon.InCr()
		defer r.mon.Decr()
		// 简单一点，如果请求头中携带vip说明是vip用户
		v := c.GetHeader("vip")
		if v != "" {
			c.Request = c.Request.WithContext(CtxWithVip(c.Request.Context()))
		}
		ok, err := r.limit.Limit(c.Request.Context())
		if err != nil {
			c.String(http.StatusInternalServerError, "系统错误")
			return
		}
		if ok {
			c.String(http.StatusInternalServerError, "你被限流了")
			return
		}
		c.Next()
	}
}

type BizHandler struct {
	count int64
}

func (b *BizHandler) RegisterRouter(server *gin.Engine) {
	server.GET("/ratelimit", func(c *gin.Context) {
		// 模拟处理时间
		t := 500 + rand.IntN(1000)
		time.Sleep(time.Duration(t) * time.Millisecond)
	})
}

func StartServer(addr string, middlewares ...gin.HandlerFunc) {
	// 不输出gin的日志
	gin.DefaultWriter = ioutil.Discard
	r := gin.Default()
	r.Use(middlewares...)
	hdl := BizHandler{}
	hdl.RegisterRouter(r)
	r.Run(addr)
}
