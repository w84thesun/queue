package queue

import (
	"log"
)

// Queue provides concurrent-safe queue mechanism that is split by keys and organized with priorities.
type Queue struct {
	// List of sequences
	sequences map[string]*Sequence

	// Makes Add() concurrent-safe by processing requests one-by-one
	// Channel should be fast that mutex
	requests chan Job

	// Delete used to delete drained sequences.
	// If drained sequence is found on Add(),
	drained chan string

	// Used to break Run() cycle
	stopCh chan struct{}
}

type Action func()

func NewQueue() *Queue {
	return &Queue{
		sequences: map[string]*Sequence{},
		requests:  make(chan Job),
		drained:   make(chan string),
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
			q.handleRequest(job)
		case key := <-q.drained:
			delete(q.sequences, key)
		case <-q.stopCh:
			break cycle
		}
	}

	log.Println("queue stopped")
}

func (q *Queue) handleRequest(job Job) {
	seq, ok := q.sequences[job.SequenceKey]
	if !ok {
		// Prepare and start new sequence
		seq = NewSequence(job.SequenceKey, q.drained)
		q.sequences[job.SequenceKey] = seq
		go seq.Run()
	}

	// Add to existing sequence.
	// If existing sequence is drained, delete it and call handleRequest again, it will recreate sequence
	err := seq.Add(job.Priority, job.Unique, job.Action)
	if err != nil {
		if err == ErrDrained {
			delete(q.sequences, job.SequenceKey)
			q.handleRequest(job)
		}
	}
}

// Stops queue by trying to break Run() cycle
func (q *Queue) Stop() {
	q.stopCh <- struct{}{}
}

// Entry point to Sequence.
func (q *Queue) Add(job Job) {
	q.requests <- job
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
