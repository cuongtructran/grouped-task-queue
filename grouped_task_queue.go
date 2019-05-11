package gtq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type GroupedTaskQueue struct {
	taskChannelMap sync.Map
	groupSize      int
}

type taskChannel struct {
	channel chan func()
	cf      context.CancelFunc
}

func (g *GroupedTaskQueue) Submit(group string, task func()) error {
	tc, ok := g.taskChannelMap.LoadOrStore(group, &taskChannel{
		make(chan func(), g.groupSize),
		nil,
	})

	if len(tc.(*taskChannel).channel) >= g.groupSize {
		return errors.New(fmt.Sprintf("Group %s is full, can't add more tasks", group))
	}

	tc.(*taskChannel).channel <- task
	if !ok {
		g.startExecuting(tc.(*taskChannel))
	}

	return nil
}

func (g *GroupedTaskQueue) Get(group string) *taskChannel {
	tc, ok := g.taskChannelMap.Load(group)
	if !ok {
		return nil
	}
	return tc.(*taskChannel)
}

func (g *GroupedTaskQueue) Destroy() {
	g.taskChannelMap.Range(func(key, value interface{}) bool {
		v := value.(*taskChannel).channel
		close(v)
		return true
	})
}

func (g *GroupedTaskQueue) startExecuting(tc *taskChannel) {
	taskCtx, cf := context.WithCancel(context.Background())
	tc.cf = cf
	go g.execute(tc, taskCtx)
}

func (g *GroupedTaskQueue) stopExecuting(tc *taskChannel) {
	if tc.cf != nil {
		tc.cf()
	}
}

func (g *GroupedTaskQueue) execute(tc *taskChannel, ctx context.Context) {
	defer tc.cf()
	for {
		select {
		case <-ctx.Done():
			break
		case task := <-tc.channel:
			if task != nil {
				task()
			}
		}
	}
}

func NewGroupedDispatchQueue(groupSize int) *GroupedTaskQueue {
	return &GroupedTaskQueue{
		sync.Map{},
		groupSize,
	}
}
