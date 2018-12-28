package membatch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func buildEchoFuncWithDelay(delay time.Duration) Func {
	return func(ctx context.Context, tasks map[string]map[string]interface{}) (resp map[string]interface{}, err error) {
		if delay > 0 {
			time.Sleep(delay)
		}

		resp = make(map[string]interface{})
		for id := range tasks {
			resp[id] = id
		}

		return resp, nil
	}
}

func TestBatch_Insert(t *testing.T) {
	tt := []struct {
		caseName          string
		flushMaxSize      int
		flushMaxWait      time.Duration
		workerSize        int
		wantProcTurnCount int
	}{
		{
			caseName:          "normal one worker match max flush size",
			flushMaxSize:      2,
			flushMaxWait:      time.Millisecond * 100,
			workerSize:        1,
			wantProcTurnCount: 5,
		},
		{
			caseName:          "normal two worker match max flush size",
			flushMaxSize:      2,
			flushMaxWait:      time.Millisecond * 100,
			workerSize:        2,
			wantProcTurnCount: 3,
		},
		{
			caseName:          "normal one worker match max flush wait",
			flushMaxSize:      20,
			flushMaxWait:      time.Millisecond * 100,
			workerSize:        1,
			wantProcTurnCount: 2,
		},
		{
			caseName:          "normal two worker match max flush wait",
			flushMaxSize:      20,
			flushMaxWait:      time.Millisecond * 100,
			workerSize:        2,
			wantProcTurnCount: 2,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			b := New(context.Background(), buildEchoFuncWithDelay(0),
				item.flushMaxSize, item.flushMaxWait, item.workerSize)

			begin := time.Now()
			var wg sync.WaitGroup
			resps := make([]*TaskResp, 10)
			wg.Add(len(resps))
			for i := 0; i < len(resps); i++ {
				go func(idx int) {
					defer wg.Done()

					ch := make(chan *TaskResp)
					err := b.Insert(TaskArgs{ID: fmt.Sprint(idx), RespChan: ch})

					if err != nil {
						t.Fatal(err)
					}

					resps[idx] = <-ch
				}(i)
			}
			wg.Wait()

			procDuration := time.Now().Sub(begin)
			gotNumOfFlushWait := int(procDuration / item.flushMaxWait)
			if gotNumOfFlushWait > item.wantProcTurnCount {
				t.Errorf("proccess time, number of flush max wait: want=%v, got=%v",
					item.wantProcTurnCount, procDuration)
			} else {
				t.Log("proccess time, number of flush max wait:", procDuration)
			}

			for i, resp := range resps {
				if resp.Data != fmt.Sprint(i) {
					t.Errorf("resp out value, want=%v, got=%v", i, resp.Data)
				}
			}
		})

	}
}

func TestBatch_ForceFlush(t *testing.T) {
	b := New(context.Background(), buildEchoFuncWithDelay(0), 10, time.Second*100, 1)

	respChan1 := make(chan *TaskResp)
	b.Insert(TaskArgs{
		ID:       "1",
		RespChan: respChan1,
	})

	respChan2 := make(chan *TaskResp)
	b.Insert(TaskArgs{
		ID:       "2",
		RespChan: respChan2,
	})

	if err := b.ForceFlush(); err != nil {
		t.Fatal(err)
	}

	task1 := <-respChan1
	task2 := <-respChan2

	if task1.Data != "1" || task2.Data != "2" {
		t.Errorf("out value: want=[1, 2], got=[%v, %v]", task1.Data, task2.Data)
	}
}

func TestBatch_Stop(t *testing.T) {
	b := New(context.Background(), buildEchoFuncWithDelay(0), 10, time.Second*100, 1)

	b.Stop()
	if err := b.Insert(TaskArgs{}); err != nil {
		t.Log(err)
	} else {
		t.Error("insert return nil err after stopped")
	}

	if err := b.ForceFlush(); err != nil {
		t.Log(err)
	} else {
		t.Error("force flush return nil err after stopped")
	}

	if err := b.Stop(); err != nil {
		t.Log(err)
	} else {
		t.Error("stop return nil err after stopped")
	}
}

func TestBatch_InsertAndWaitResp(t *testing.T) {
	b := New(context.Background(), buildEchoFuncWithDelay(0), 10, time.Millisecond*100, 1)
	resp, err := b.InsertAndWaitResp("1", nil)
	if err != nil {
		t.Fatal(err)
	}

	if resp != "1" {
		t.Errorf("out value: want=%v, got=%v", 1, resp)
	}
}
