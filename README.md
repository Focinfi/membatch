membatch
------
Batch processing in golang, inspired by [gobatch](https://github.com/herryg91/gobatch).

### Install 
```bash
go get github.com/Focinfi/membatch
```

### Demo
```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/Focinfi/membatch"
)

func main() {
	// 1. options
	ctx := context.TODO()
	// tasks: task_id => params
	// resp: task_id => data
	echoFunc := func(ctx context.Context, tasks map[string]map[string]interface{}) (resp map[string]interface{}, err error) {
		resp = make(map[string]interface{})
		for taskID := range tasks {
			resp[taskID] = taskID
		}
		return
	}
	flushMaxSize := 100         // process threshold for the count of waiting tasks
	flushMaxWait := time.Second // process threshold for waiting duration
	workerNum := 2              // the number of gorountine to handling task

	// 2. build a new batch
	batch := membatch.New(ctx, echoFunc, flushMaxSize, flushMaxWait, workerNum)

	// 3. one task args
	respChan := make(chan *membatch.TaskResp)
	taskArgs := membatch.TaskArgs{
		ID:       "1",
		RespChan: respChan,
	}

	// 4. insert one task
	if err := batch.Insert(taskArgs); err != nil {
		log.Fatal(err)
	}

	// 5. wait for response
	resp := <-respChan

	// 6. println 1 after one second
	log.Println(resp.Data)
}
```
