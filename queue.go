package queue

import "log"

// Queue provides concurrent-safe queue mechanism that is split by keys and organized with priorities.
// Requirements:
//   1. Concurrent-safe
// 	 2. Fast - minimum await time between jobs and on adding new tasks
//   3. Execution split by keys: tasks with same keys are executed sequentially,
//      while different keys results in parallel execution
//   4. Tasks action within single sequence must be ordered using priority value,
//      where smaller value means higher priority
//   5. Sequence jobs can be unique, means if there is queued unique job,
//      attempt to add another unique job will do nothing

type Queue struct {
	sequences map[string]*Sequence

	// Makes Add() concurrent-safe by processing requests one-by-one
	// Channel should be fast that mutex
	requests chan Job

	// identifier
	killCh chan string

	stopCh chan struct{}
}

type Action func()

func NewQueue() *Queue {
	return &Queue{
		sequences: map[string]*Sequence{},
		requests:  make(chan Job),
		killCh:    make(chan string),
		stopCh:    make(chan struct{}),
	}
}

// Handle requests chan
// Exits on channel close
func (q *Queue) Run() {
cycle:
	for {
		select {
		case job := <-q.requests:
			seq, ok := q.sequences[job.SequenceKey]
			if ok {
				seq.Add(job)
				break
			}

			newSeq := NewSequence(job.SequenceKey, q.killCh)
			newSeq.Add(job)

			q.sequences[job.SequenceKey] = newSeq
			newSeq.Continue()

		case killKey := <-q.killCh:
			seq := q.sequences[killKey]
			if seq.Len() > 0 {
				seq.Continue()
				break
			}
			delete(q.sequences, killKey)
		case <-q.stopCh:
			break cycle
		}
	}

	log.Println("queue stopped")
}

func (q *Queue) Stop() {
	q.stopCh <- struct{}{}
}

// Entry point to Sequence.
// queueKey is used to differentiate sequences,
// e.g. if we want to process match operations one-by-one, matchID should be used
// lower priority value means higher execution priority, e.g. priority 1 means high, 2 is medium, 3 is low etc
// do is func that will be executed
func (q *Queue) Add(job Job) {
	q.requests <- job
}

func (q *Queue) Add2(sequenceKey string, priority int, unique string, action Action) {
	q.requests <- Job{
		SequenceKey: sequenceKey,
		Priority:    priority,
		Unique:      unique,
		Action:      action,
	}
}

type Job struct {
	// SequenceKey is used to differentiate sequences
	SequenceKey string

	// Smaller the number, higher the priority, e.g. priority 1 means high, 2 is medium, 3 is low etc
	Priority int

	// Code to execute
	Action Action

	// Unique is used to decide if job can be added to current sequence.
	// If there is already job with same uniqueKey, job will be ignored
	Unique string
}
