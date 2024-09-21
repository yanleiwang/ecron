package scheduler

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

type PreemptScheduler struct {
	executionDAO      storage.ExecutionDAO
	taskCfgRepository storage.TaskCfgRepository
	executors         map[string]executor.Executor
	refreshInterval   time.Duration
	limiter           *semaphore.Weighted
	logger            *slog.Logger
	pe                preempt.Preempter
}

func NewPreemptScheduler(executionDAO storage.ExecutionDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger,
	preempter preempt.Preempter, taskCfgRepository storage.TaskCfgRepository) *PreemptScheduler {
	return &PreemptScheduler{
		executionDAO:      executionDAO,
		refreshInterval:   refreshInterval,
		limiter:           limiter,
		executors:         make(map[string]executor.Executor),
		logger:            logger,
		pe:                preempter,
		taskCfgRepository: taskCfgRepository,
	}
}

func (p *PreemptScheduler) RegisterExecutor(execs ...executor.Executor) {
	for _, exec := range execs {
		p.executors[exec.Name()] = exec
	}
}

func (p *PreemptScheduler) Schedule(ctx context.Context) error {
	for {

		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		timeout, cancel := context.WithTimeout(ctx, time.Second*3)
		leaser, err := p.pe.Preempt(timeout)
		cancel()
		if err != nil {
			p.logger.Error("抢占任务失败,可能没有任务了",
				slog.Any("error", err))
			p.limiter.Release(1)
			time.Sleep(time.Second * 3)
			continue
		}

		t := leaser.GetTask()
		exec, ok := p.executors[t.Executor]
		if !ok {
			p.logger.Error("找不到任务的执行器",
				slog.Int64("TaskID", t.ID),
				slog.String("Executor", t.Executor))
			p.limiter.Release(1)
			timeout, cancel := context.WithTimeout(ctx, time.Second*3)
			err = leaser.Release(ctx)
			if err != nil {
				cancel()
				p.logger.Error("于抢占后释放任务失败",
					slog.Int64("TaskID", t.ID),
					slog.Any("err", err))
				continue
			}

			// 找不到执行器 感觉也应该 updateNextTime?
			_ = p.updateNextTime(timeout, t, time.Now())
			cancel()
			continue
		}

		go p.doTaskWithAutoRefresh(ctx, leaser, exec)
	}
}

func (p *PreemptScheduler) doTaskWithAutoRefresh(ctx context.Context, l preempt.TaskLeaser, exec executor.Executor) {
	t := l.GetTask()

	defer func() {
		err := p.ReleaseTask(l, t, exec)
		if err != nil {
			p.logger.Error("任务释放失败", slog.Int64("task_id", t.ID),
				slog.Any("err", err))
		}

	}()

	cancelCtx, cancelCause := context.WithCancelCause(ctx)
	ch, err := l.AutoRefresh(cancelCtx)
	defer cancelCause(nil)

	if err != nil {
		cancelCause(err)
		return
	}

	go func() {
		for {
			s, ok := <-ch
			if ok && s.Err() != nil {
				cancelCause(s.Err())
				return
			}
		}
	}()

	timeout := exec.TaskTimeout(t)
	execCtx, execCancel := context.WithTimeout(cancelCtx, timeout)
	defer execCancel()

	// 如果任务的上次执行状态是 Running 说明上次调度是非正常中断
	// 本次 调度 只会 尝试去 获取 上次任务的执行结果, 而不真正执行 任务
	if t.LastStatus == task.TaskStatusRunning {
		lastExecution, err, done := p.getLastExecution(execCtx, t)
		if done {
			return
		}
		t.CurExecution = &lastExecution
		// 探查任务的执行情况
		err = p.exploreLastExecution(execCtx, t, exec)
		if err != nil {
			p.logger.Error("探查最近一次任务执行记录失败", slog.Int64("task_id", t.ID),
				slog.Int64("eid", t.CurExecution.ID), slog.Any("err", err))
		}
		return
	}

	execution, err := p.executionDAO.Create(execCtx, t.ID)
	if err != nil {
		return
	}

	t.CurExecution = &execution

	err = p.doTask(execCtx, t, exec)

	if err != nil {
		p.logger.Error("任务执行失败", slog.Int64("task_id", t.ID),
			slog.Int64("eid", t.CurExecution.ID), slog.Any("err", err))
	}

}

func (p *PreemptScheduler) getLastExecution(execCtx context.Context, t task.Task) (task.Execution, error, bool) {
	// 先获取任务最近的一次执行记录
	lastExecution, err := p.executionDAO.GetLastExecution(execCtx, t.ID)
	if err != nil {
		return task.Execution{}, err, true
	}
	// 执行记录 显示 执行 已经结束了， 说明 只是release失败
	if lastExecution.Status != task.ExecStatusRunning {
		// 重新计算下 nextTime, err了 也无所谓
		_ = p.updateNextTime(execCtx, t, lastExecution.Utime)
		return task.Execution{}, nil, true
	}
	return lastExecution, nil, false
}

func (p *PreemptScheduler) ReleaseTask(l preempt.TaskLeaser, t task.Task, exec executor.Executor) error {
	p.limiter.Release(1)
	nctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := l.Release(nctx)
	if err != nil {
		return err
	}
	if t.CurExecution == nil {
		return nil
	}
	err = p.updateProgressStatus(nctx, t.CurExecution.ID, int(t.CurExecution.Progress), t.CurExecution.Status)
	if err != nil {
		return err
	}
	err = p.updateNextTime(nctx, t, time.Now())
	if err != nil {
		return err
	}

	if t.CurExecution.Status != task.ExecStatusDeadlineExceeded && t.CurExecution.Status != task.ExecStatusCancelled {
		return nil
	}

	return exec.Stop(nctx, t, t.CurExecution.ID)

}

func (p *PreemptScheduler) doTask(ctx context.Context, t task.Task, exec executor.Executor) error {
	status, err := exec.Run(ctx, t, t.CurExecution.ID)
	t.CurExecution.Status = status
	if status == task.ExecStatusSuccess {
		t.CurExecution.Progress = 100
	}
	if err != nil || status != task.ExecStatusRunning {
		return err
	}

	return p.explore(ctx, exec, t)

}

func (p *PreemptScheduler) exploreLastExecution(ctx context.Context, t task.Task, exec executor.Executor) error {
	err := p.exploreOnce(ctx, exec, t)
	if err != nil {
		return err
	}

	if t.CurExecution.Status != task.ExecStatusRunning {
		return nil
	}
	//  调整 执行超时时间 = 任务记录 创建时间 + 最大执行时间
	expectStopTime := t.CurExecution.Ctime.Add(exec.TaskTimeout(t))
	nctx, cancel := context.WithDeadline(ctx, expectStopTime)
	defer cancel()

	//继续 探查
	return p.explore(nctx, exec, t)

}

func (p *PreemptScheduler) exploreOnce(ctx context.Context, exec executor.Executor, t task.Task) error {
	nctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eid := t.CurExecution.ID
	ch := exec.Explore(nctx, eid, t)
	if ch == nil {
		t.CurExecution.Status = task.ExecStatusUnknown
		return errs.ErrTaskNotSupportExplore
	}

	select {
	case <-ctx.Done():
		t.CurExecution.Status = task.ExecStatusUnknown
		return ctx.Err()
	case res := <-ch:
		t.CurExecution.Status = p.from(res.Status)
		return nil
	}

}

func (p *PreemptScheduler) explore(ctx context.Context, exec executor.Executor, t task.Task) error {
	eid := t.CurExecution.ID
	ch := exec.Explore(ctx, eid, t)
	if ch == nil {
		t.CurExecution.Status = task.ExecStatusUnknown
		return errs.ErrTaskNotSupportExplore
	}
	// 保存每一次探查时的进度，确保执行ctx.Done()分支时进度不会更新为零值
	progress := 0
	status := task.ExecStatusUnknown
	for {
		select {
		case <-ctx.Done():
			// 主动取消或者超时
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				status = task.ExecStatusDeadlineExceeded
			} else {
				status = task.ExecStatusCancelled
			}
		case res, ok := <-ch:
			if !ok {
				select {
				case <-ctx.Done():
					continue
				default:
					status = task.ExecStatusUnknown
				}
			} else {
				progress = res.Progress
				status = p.from(res.Status)
			}

		}

		t.CurExecution.Status = status
		t.CurExecution.Progress = uint8(progress)
		err := p.updateProgressStatus(ctx, eid, progress, status)
		if err != nil || status != task.ExecStatusRunning {
			return err
		}
	}
}

func (p *PreemptScheduler) from(status executor.Status) task.ExecStatus {
	switch status {
	case executor.StatusSuccess:
		return task.ExecStatusSuccess
	case executor.StatusFailed:
		return task.ExecStatusFailed
	default:
		return task.ExecStatusRunning
	}
}

func (p *PreemptScheduler) updateNextTime(ctx context.Context, t task.Task, time2 time.Time) error {
	next, err := t.NextTime(time2)
	if err != nil {
		p.logger.Error("计算任务下一次执行时间失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
		return err
	}

	if next.IsZero() {
		err = p.taskCfgRepository.Stop(ctx, t.ID)
		if err != nil {
			p.logger.Error("停止任务调度失败",
				slog.Int64("TaskID", t.ID),
				slog.Any("error", err))
		}
		return err
	}
	err = p.taskCfgRepository.UpdateNextTime(ctx, t.ID, next)
	if err != nil {
		p.logger.Error("更新下一次执行时间出错",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
	return err
}

func (p *PreemptScheduler) updateProgressStatus(ctx context.Context, eid int64, progress int, status task.ExecStatus) error {
	err := p.executionDAO.Update(ctx, eid, status, progress)
	if err != nil {
		p.logger.Error("更新任务记录失败", slog.Int64("execution_id", eid),
			slog.String("exec_status", status.String()), slog.Int("progress", progress),
			slog.Any("error", err))
	}
	return err
}
