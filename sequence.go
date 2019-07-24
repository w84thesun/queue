package queue

import (
	"sync"
	"time"
)

type Sequence struct {
	// Sequence key, used to delete Sequence from Queue
	key string

	// Sequence will be deleted when key is passed through this channel
	killCh chan<- string

	cancelKillCh chan struct{}

	m sync.Mutex

	// List of ordered jobs
	jobs []seqJob

	// By default when jobs list is exhausted, sequence tries to kill itself immediately.
	// IdleTTL defines pause before jobs exhaustion and suicide attempt.
	idleTTL time.Duration
}

func NewSequence(key string, idleTTL time.Duration, killCh chan<- string) *Sequence {
	return &Sequence{
		key:          key,
		killCh:       killCh,
		cancelKillCh: make(chan struct{}),
		jobs:         []seqJob{},
		idleTTL:      idleTTL,
	}
}

func (s *Sequence) Add(priority int, unique string, action Action) {
	s.m.Lock()
	defer s.m.Unlock()

	// Reject unique duplicates
	if unique != "" {
		exists := findDuplicates(s.jobs, unique)
		if exists {
			return
		}
	}

	// find out position to insert job
	i := findInsertIndex(s.jobs, priority)

	job := seqJob{
		priority: priority,
		unique:   unique,
		action:   action,
	}

	s.jobs = insert(s.jobs, i, job)

	// Cancel kill attempts if active
	select {
	case s.cancelKillCh <- struct{}{}:
	default:
	}
}

func findInsertIndex(jobs []seqJob, priority int) (idx int) {
	for i, job := range jobs {
		if priority < job.priority {
			return i
		}
	}

	return len(jobs)
}

type seqJob struct {
	priority int
	unique   string
	action   Action
}

// look for jobs with same unique key
func findDuplicates(jobs []seqJob, unique string) bool {
	for _, job := range jobs {
		if job.unique == unique {
			return true
		}
	}
	return false
}

// Insert without new slice creation https://github.com/golang/go/wiki/SliceTricks#insert
func insert(jobs []seqJob, i int, job seqJob) []seqJob {
	jobs = append(jobs, seqJob{})
	copy(jobs[i+1:], jobs[i:])
	jobs[i] = job

	return jobs
}

// Kill is called when there are no jobs remaining.
// It tries to kill Sequence by passing sequence key to outer Queue.
// Kill can be cancelled if query adds another job for this sequence, which will send msg to addedCh
func (s *Sequence) kill() {
	select {
	case s.killCh <- s.key:
	case <-s.cancelKillCh:
	}
}

// Continue calls processes next Job in sequence
// If there are no jobs left sequence initiates
func (s *Sequence) Continue() {
	job, ok := s.shift()
	if !ok {
		s.kill()
		return
	}

	go func() {
		job.action()
		s.Continue()
	}()
}

func (s *Sequence) shift() (job seqJob, ok bool) {
	s.m.Lock()
	defer s.m.Unlock()

	if len(s.jobs) == 0 {
		return seqJob{}, false
	}

	job, s.jobs = s.jobs[0], s.jobs[1:]

	return job, true
}
