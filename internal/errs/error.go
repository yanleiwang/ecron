package errs

import "errors"

var (
	ErrExecuteTaskFailed = errors.New("任务执行失败")
	ErrInCorrectConfig   = errors.New("任务配置信息错误")
	ErrRequestFailed     = errors.New("发起任务请求失败")
	ErrRequestTimeout    = errors.New("发起任务请求超时")
	ErrUnknownTask       = errors.New("未知的任务类型")

	ErrNoExecutableTask = errors.New("当前没有可执行的任务")
)
