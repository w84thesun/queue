package queue

import (
	"errors"
	"sync"
)

type Sequence struct {
	// Sequence key, used to delete Sequence from Queue
	key string

	// First added messaged only once after first Add() call
	// Run() will wait for this message before processing jobs, this allows better flow in handleRequest()
	firstAdded chan struct{}
	once       sync.Once

	m sync.Mutex

	// Marks sequence as drained, i.e. at the end of life, rejecting any Add requests and pending deletion.
	drained bool

	// List of ordered jobs
	jobs []seqJob

	// Pass sequence key here to delete it from pool
	delete chan<- string
}

func NewSequence(key string, delete chan<- string) *Sequence {
	return &Sequence{
		key:        key,
		firstAdded: make(chan struct{}),
		jobs:       []seqJob{},
		delete:     delete,
	}
}

func (s *Sequence) Run() {
	<-s.firstAdded

	for {
		s.m.Lock()

		job, found := s.shift()
		if !found {
			s.drained = true
			s.m.Unlock()

			break
		}

		s.m.Unlock()

		job.action()
	}

	s.delete <- s.key
}

var (
	ErrDuplicate = errors.New("duplicate")
	errDrained   = errors.New("drainedCh")
)

func (s *Sequence) Add(priority int, unique string, action Action) error {
	s.m.Lock()
	defer s.m.Unlock()

	// If sequence is just created and this is its first Add() call, signal firstAdded for Run()
	defer s.once.Do(func() { s.firstAdded <- struct{}{} })

	if s.drained {
		return errDrained
	}

	// Reject unique duplicates
	if unique != "" {
		exists := findDuplicates(s.jobs, unique)
		if exists {
			return ErrDuplicate
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

	return nil
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

// Shift (or front pop) pulls out first job and shift other elements to the left.
// E.g. shift() on [5,2,6,7,4] will return 5 and change slice to [2.6.7.4]
// Will return false if no jobs left
func (s *Sequence) shift() (job seqJob, ok bool) {
	if len(s.jobs) == 0 {
		return seqJob{}, false
	}

	job, s.jobs = s.jobs[0], s.jobs[1:]

	return job, true
}
