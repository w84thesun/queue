package queue_test

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"gitlab.nbaplus.tk/backend/queue"
)

func TestQueue_RunPi(t *testing.T) {
	q := queue.NewQueue()
	go q.Run()

	priorities := map[string][]int{
		"alpha": {
			3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3,
			8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9, 5, 0, 2, 8, 8,
			4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0, 5, 8, 2,
			0, 9, 7, 4, 9, 4, 4, 5, 9, 2, 3, 0, 7, 8, 1, 6, 4, 0,
		},
		"beta": {
			3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3,
			8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9, 5, 0, 2, 8, 8,
			4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0, 5, 8, 2,
			0, 9, 7, 4, 9, 4, 4, 5, 9, 2, 3, 0, 7, 8, 1, 6, 4, 0,
		},
		"delta": {
			3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3,
			8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9, 5, 0, 2, 8, 8,
			4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0, 5, 8, 2,
			0, 9, 7, 4, 9, 4, 4, 5, 9, 2, 3, 0, 7, 8, 1, 6, 4, 0,
		},
		"gamma": {
			3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3,
			8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9, 5, 0, 2, 8, 8,
			4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0, 5, 8, 2,
			0, 9, 7, 4, 9, 4, 4, 5, 9, 2, 3, 0, 7, 8, 1, 6, 4, 0,
		},
		"gamma1": {
			3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3,
			8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9, 5, 0, 2, 8, 8,
			4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0, 5, 8, 2,
			0, 9, 7, 4, 9, 4, 4, 5, 9, 2, 3, 0, 7, 8, 1, 6, 4, 0,
		},
		"rho":      {2},
		"sigma":    {2},
		"upsilon":  {2},
		"phi":      {2},
		"chi":      {2},
		"psi":      {2},
		"omega":    {2},
		"rho1":     {2},
		"sigma1":   {2},
		"upsilon1": {2},
		"phi1":     {2},
		"chi1":     {2},
		"psi1":     {2},
		"omega1":   {2},
	}

	wg := sync.WaitGroup{}

	started := time.Now()

	for key, values := range priorities {
		for range values {
			wg.Add(1)

			q.Add(queue.Job{
				SequenceKey: key,
				Priority:    1,
				Action: func() {
					time.Sleep(time.Millisecond)
					wg.Done()
				},
			})
		}
	}

	log.Printf("all added %v", time.Since(started))

	wg.Wait()
	log.Println("stopping")
	q.Stop()
}

func TestQueue_Run(t *testing.T) {
	q := queue.NewQueue()
	go q.Run()

	wg := sync.WaitGroup{}

	q.Add(queue.Job{
		SequenceKey: "match 1",
		Priority:    High,
		Action: func() {
			time.Sleep(10 * time.Millisecond)
			log.Println("match 1: add match")
		},
	})

	q.Add(queue.Job{
		SequenceKey: "match 1",
		Priority:    Medium,
		Action: func() {
			time.Sleep(10 * time.Millisecond)
			log.Println("match 1: add event")
		},
	})

	wg.Add(1)
	q.Add(queue.Job{
		SequenceKey: "match 1",
		Priority:    Low,
		Action: func() {
			time.Sleep(10 * time.Millisecond)
			log.Println("match 1: recalculate")
		},
		Unique: "recalculate",
	})

	q.Add(queue.Job{
		SequenceKey: "match 1",
		Priority:    Medium,
		Action: func() {
			time.Sleep(10 * time.Millisecond)
			log.Println("match 1: add event")
		},
	})

	q.Add(queue.Job{
		SequenceKey: "match 1",
		Priority:    Low,
		Action: func() {
			time.Sleep(10 * time.Millisecond)
			log.Println("match 1: recalculate")
		},
		Unique: "recalculate",
	})

	time.Sleep(time.Second)
}

const (
	High int = iota + 1
	Medium
	Low
)

func BenchmarkQueue(b *testing.B) {
	benches := []int{1, 2, 5, 10, 20, 50, 100, 1000, 5000, 9000}

	for _, n := range benches {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkQueueN(b, n)
		})
	}
}

func benchmarkQueueN(b *testing.B, n int) {
	q := queue.NewQueue()
	go q.Run()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(n)

		for j := 0; j < n; j++ {
			q.Add(queue.Job{
				SequenceKey: "1",
				Priority:    i,
				Action: func() {
					wg.Done()
				},
			})
		}
		wg.Wait()
	}
}
