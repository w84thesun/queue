package queue

import (
	"errors"
)

// Queue provides concurrent-safe queue mechanism that is split by keys and organized with priorities.
type Queue struct {
	// List of sequences
	sequences map[string]*Sequence

	// Makes Add() concurrent-safe by processing requests one-by-one
	// Channel should be faster than mutex
	requests chan jobRequest

	// used to delete drained sequences.
	drainedCh chan string

	// Used to break Run() cycle
	stopCh chan StopStrategy

	// Will be closed when .Run() is out of cycle
	stopped chan struct{}
}

type StopStrategy int

const (
	// Stop queue and terminate all pending sequences immediately
	Immediate StopStrategy = 1

	// Try to finish all sequences and only then block new jobs.
	// Can be useful for cases with chaining queue calls (i.e. A queues B, B queues C),
	//  but can possibly run indefinitely if jobs keep incoming, so its on client side to stop them
	Drain StopStrategy = 2
)

type Action func()

func NewQueue() *Queue {
	return &Queue{
		sequences: map[string]*Sequence{},
		requests:  make(chan jobRequest),
		drainedCh: make(chan string),
		stopCh:    make(chan StopStrategy),
		stopped:   make(chan struct{}),
	}
}

// Handle requests chan
// Exits on channel close
func (q *Queue) Run() {
	// Stopping used to exit run once all sequences were drained
	var stopStrategy StopStrategy

cycle:
	for {
		select {
		case req := <-q.requests:
			q.handleRequest(req)

		case key := <-q.drainedCh:
			delete(q.sequences, key)

			if stopStrategy == Drain && len(q.sequences) == 0 {
				break cycle
			}

		case stopStrategy = <-q.stopCh:
			switch stopStrategy {
			case Immediate:
				q.terminateAllSequences()

				break cycle

			case Drain:
				// Check if there are not sequences so we can stop immediately
				if len(q.sequences) == 0 {
					break cycle
				}
			}
		}
	}

	close(q.stopped)
}

func (q *Queue) handleRequest(req jobRequest) {
	seq, ok := q.sequences[req.job.SequenceKey]
	if !ok {
		// Prepare and start new sequence
		seq = NewSequence(req.job.SequenceKey, q.drainedCh)
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

func (q *Queue) Stop() {
	q.stop(Immediate)
}

func (q *Queue) GracefulStop() {
	q.stop(Drain)
}

func (q *Queue) stop(strategy StopStrategy) {
	q.stopCh <- strategy
	<-q.stopped
}

func (q *Queue) terminateAllSequences() {
	for _, seq := range q.sequences {
		seq.Terminate()
	}
}

var ErrQueueStopped = errors.New("queue is stopped")

// Entry point to Sequence.
// Could fail if unique job is duplicated or queue is stopping/stopped.
func (q *Queue) Add(job Job) (err error) {
	reply := make(chan error)
	select {
	case q.requests <- jobRequest{job: job, reply: reply}:
	case <-q.stopped:
		return ErrQueueStopped
	}

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
