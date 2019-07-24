package queue

import (
	"errors"
	"sync"
)

type Sequence struct {
	// Sequence key, used to delete Sequence from Queue
	key string

	firstAdded chan struct{}
	once       sync.Once

	m sync.Mutex

	drained bool

	// List of ordered jobs
	jobs []seqJob

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
	ErrDrained   = errors.New("drained")
)

func (s *Sequence) Add(priority int, unique string, action Action) error {
	s.m.Lock()
	defer s.m.Unlock()
	defer s.once.Do(func() { s.firstAdded <- struct{}{} })

	if s.drained {
		return ErrDrained
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

func (s *Sequence) shift() (job seqJob, ok bool) {
	if len(s.jobs) == 0 {
		return seqJob{}, false
	}

	job, s.jobs = s.jobs[0], s.jobs[1:]

	return job, true
}
