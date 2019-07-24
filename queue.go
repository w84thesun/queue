package queue

import (
	"log"
	"time"
)

// Queue provides concurrent-safe queue mechanism that is split by keys and organized with priorities.
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
				seq.Add(job.Priority, job.Unique, job.Action)
				break
			}

			newSeq := NewSequence(job.SequenceKey, time.Second, q.killCh)
			newSeq.Add(job.Priority, job.Unique, job.Action)

			q.sequences[job.SequenceKey] = newSeq
			newSeq.Continue()

		case killKey := <-q.killCh:
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
