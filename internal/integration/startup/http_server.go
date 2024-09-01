package startup

import (
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"time"
)

func StartHttpServer(port string) {
	server := gin.Default()
	h := new(HttpServer)
	h.RegisterRouter(server)
	err := server.Run(":" + port)
	if err != nil {
		panic(err)
	}
}

// HttpServer 模拟业务方的返回
type HttpServer struct {
	// 记录当前是第几个请求
	count int
}

func (u *HttpServer) RegisterRouter(server *gin.Engine) {
	server.GET("/success", u.Success)
	server.GET("/failed", u.Fail)
	server.GET("/error", u.Error)
	server.GET("/timeout", u.Timeout)
	server.GET("/explore_success", u.ExploreSuccess)
	server.GET("/explore_failed", u.ExploreFailed)
	server.GET("/running", u.Running)
}

func (u *HttpServer) Success(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusSuccess,
		Progress: 100,
	})
}

func (u *HttpServer) ExploreSuccess(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	if u.count == 0 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 10,
		})
		u.count++
		return
	} else if u.count == 1 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 50,
		})
		u.count++
		return
	} else {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusSuccess,
			Progress: 100,
		})
	}
}

func (u *HttpServer) ExploreFailed(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	if u.count == 0 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 10,
		})
		u.count++
		return
	} else if u.count == 1 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 50,
		})
		u.count++
		return
	} else {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusFailed,
			Progress: 50,
		})
	}
}

func (u *HttpServer) Fail(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusFailed,
		Progress: 0,
	})
}

func (u *HttpServer) Error(ctx *gin.Context) {
	ctx.AbortWithStatus(http.StatusServiceUnavailable)
}

func (u *HttpServer) Timeout(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	// 发起http调用是5秒，这里休眠6秒，客户端就会返回超时
	time.Sleep(time.Second * 6)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusSuccess,
		Progress: 100,
	})
}

func (u *HttpServer) Running(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusRunning,
		Progress: 10,
	})
}
