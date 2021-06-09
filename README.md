# Queue
Fast concurrent queue providing prioritized job execution.

Simple example:

```go
package main

import (
	"fmt"
	"github.com/sanyokbig/queue"
	"time"
)

func main() {
	q := queue.NewQueue()
	go q.Run()

	err := q.Add(queue.Job{
		SequenceKey: "1",
		Priority:    1,
		Action: func() {
			SomeJob(15)
		},
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(1)

	q.Stop()
}

func SomeJob(a int) {
	fmt.Println(a)
}
```
