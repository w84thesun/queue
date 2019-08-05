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
	requests chan jobRequest

	// used to delete drained sequences.
	drained chan string

	// Used to break Run() cycle
	stopCh chan struct{}
}

type Action func()

func NewQueue() *Queue {
	return &Queue{
		sequences: map[string]*Sequence{},
		requests:  make(chan jobRequest),
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
		case req := <-q.requests:
			q.handleRequest(req)
		case key := <-q.drained:
			delete(q.sequences, key)
		case <-q.stopCh:
			break cycle
		}
	}

	log.Println("queue stopped")
}

func (q *Queue) handleRequest(req jobRequest) {
	seq, ok := q.sequences[req.job.SequenceKey]
	if !ok {
		// Prepare and start new sequence
		seq = NewSequence(req.job.SequenceKey, q.drained)
		q.sequences[req.job.SequenceKey] = seq
		go seq.Run()
	}

	// Add to existing sequence.
	err := seq.Add(req.job.Priority, req.job.Unique, req.job.Action)
	if err != nil {
		// If existing sequence is drained, delete it and call handleRequest again, it will recreate sequence
		// No reply here as next handleRequest will reply
		if err == errDrained {
			delete(q.sequences, req.job.SequenceKey)
			q.handleRequest(req)
			return
		}
		req.reply <- err
		return
	}

	req.reply <- nil
}

// Stops queue by trying to break Run() cycle
func (q *Queue) Stop() {
	q.stopCh <- struct{}{}
}

// Entry point to Sequence. Returns true if job was successfully added and
// false if passed job is unique and already exists in
func (q *Queue) Add(job Job) (err error) {
	reply := make(chan error)
	q.requests <- jobRequest{job: job, reply: reply}

	return <-reply
}

type jobRequest struct {
	reply chan error
	job   Job
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
