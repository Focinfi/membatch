package membatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TaskArgs one task args
type TaskArgs struct {
	ID       string
	Args     map[string]interface{}
	RespChan chan<- *TaskResp
}

// TaskResp one task respone will be returned by TaskArgs.RespChan
type TaskResp struct {
	Err  error
	Data interface{}
}

// Func user definded batch handling functiong
// tasks is the map of task_id => task_params
// resp is the map of task_id => response_data
type Func func(ctx context.Context, tasks map[string]map[string]interface{}) (resp map[string]interface{}, err error)

// Batch main container for a batch runner
type Batch struct {
	items map[string]TaskArgs // contains inserted task
	mutex *sync.RWMutex       // mutex for updating items
	doFn  Func

	maxSize int           // task number of one batch
	maxWait time.Duration // wait duration of one batch

	flushTasks chan map[string]TaskArgs // data channel of batches to handle by workers
	isRun      bool                     // running sign

	insertChan     chan TaskArgs    // data channel to receive inserting task
	forceFlushChan chan interface{} // sign channel to flush forcely
	stopChan       chan bool        // sign channel to stop batching
}

// New creates and returns a new *Batch
func New(ctx context.Context, flushHandler Func, flushMaxSize int, flushMaxWait time.Duration, workerSize int) *Batch {
	instance := &Batch{
		items: make(map[string]TaskArgs),
		doFn:  flushHandler,
		mutex: &sync.RWMutex{},

		maxSize: flushMaxSize,
		maxWait: flushMaxWait,

		flushTasks: make(chan map[string]TaskArgs, workerSize),
		isRun:      false,

		forceFlushChan: make(chan interface{}),
		insertChan:     make(chan TaskArgs),
		stopChan:       make(chan bool),
	}

	instance.setFlushWorker(ctx, workerSize)
	instance.isRun = true
	go instance.run()
	return instance
}

// InsertAndWaitResp inserts a task with a response data channel and wait it
func (i *Batch) InsertAndWaitResp(taskID string, args map[string]interface{}) (interface{}, error) {
	respChan := make(chan *TaskResp)
	i.Insert(TaskArgs{
		ID:       fmt.Sprint(taskID),
		Args:     args,
		RespChan: respChan,
	})

	result := <-respChan
	return result.Data, result.Err
}

// Insert inserts a task
// returns non-nil err when a batch is stopped
func (i *Batch) Insert(data TaskArgs) (err error) {
	if i.isRun {
		i.insertChan <- data
	} else {
		err = errors.New("failed to insert, batch already stopped")
	}
	return
}

// ForceFlush flushes forcely to handle immediately
// returns non-nil err when a batch is stopped
func (i *Batch) ForceFlush() (err error) {
	if i.isRun {
		i.forceFlushChan <- true
	} else {
		err = errors.New("failed to force insert, batch already stopped")
	}
	return
}

// Stop stops a batch
// returns non-nil err when a batch is stopped
func (i *Batch) Stop() (err error) {
	if i.isRun {
		i.stopChan <- true
	} else {
		err = errors.New("failed to stop, batch already stopped")
	}
	return
}

// flush calls doFn to handle a batch of tasks and push response to their RespChan
func (i *Batch) flush(ctx context.Context, workerID int, tasks map[string]TaskArgs) {
	args := make(map[string]map[string]interface{}, len(tasks))
	for _, task := range tasks {
		args[task.ID] = task.Args
	}
	respData, err := i.doFn(ctx, args)
	for _, task := range tasks {
		go func(t TaskArgs) {
			t.RespChan <- &TaskResp{
				Err:  err,
				Data: respData[t.ID],
			}
		}(task)
	}
	return
}

// setFlushWorker create worker to handle batches
func (i *Batch) setFlushWorker(ctx context.Context, workerSize int) {
	for id := 1; id <= workerSize; id++ {
		go func(workerID int, flushJobs <-chan map[string]TaskArgs) {
			for j := range flushJobs {
				i.flush(ctx, workerID, j)
			}
		}(id, i.flushTasks)
	}
}

// run receives data of channels
func (i *Batch) run() {
	for {
		select {
		case <-time.Tick(i.maxWait):
			i.mutex.Lock()
			if len(i.items) > 0 {
				i.flushTasks <- i.items
				i.items = make(map[string]TaskArgs)
			}
			i.mutex.Unlock()
		case item := <-i.insertChan:
			i.mutex.Lock()
			i.items[item.ID] = item
			if len(i.items) >= i.maxSize {
				i.flushTasks <- i.items
				i.items = make(map[string]TaskArgs)
			}
			i.mutex.Unlock()
		case <-i.forceFlushChan:
			i.mutex.Lock()
			if len(i.items) > 0 {
				i.flushTasks <- i.items
				i.items = make(map[string]TaskArgs)
			}
			i.mutex.Unlock()
		case isStop := <-i.stopChan:
			if isStop {
				i.isRun = false
				return
			}
		}
	}
}
