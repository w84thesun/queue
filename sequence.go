package queue

import (
	"sync"
)

type Sequence struct {
	key    string
	killCh chan<- string

	m sync.Mutex

	jobs []Job
}

func NewSequence(key string, killCh chan<- string) *Sequence {
	return &Sequence{
		key:    key,
		killCh: killCh,
		jobs:   []Job{},
	}
}

func (s *Sequence) Add(job Job) {
	s.m.Lock()
	defer s.m.Unlock()

	if job.Unique != "" {
		exists := findDuplicates(s.jobs, job.Unique)
		if exists {
			return
		}
	}

	// find out position to insert job
	i := findInsertIndex(s.jobs, job.Priority)

	s.jobs = insert(s.jobs, i, job)
}

func findInsertIndex(jobs []Job, priority int) (idx int) {
	for i, job := range jobs {
		if priority < job.Priority {
			return i
		}
	}

	return len(jobs)
}

func findDuplicates(jobs []Job, unique string) bool {
	for _, job := range jobs {
		if job.Unique == unique {
			return true
		}
	}
	return false
}

// Insert without new slice creation https://github.com/golang/go/wiki/SliceTricks#insert
func insert(jobs []Job, i int, job Job) []Job {
	jobs = append(jobs, Job{})
	copy(jobs[i+1:], jobs[i:])
	jobs[i] = job

	return jobs
}

func (s *Sequence) Len() int {
	s.m.Lock()
	l := len(s.jobs)
	s.m.Unlock()
	return l
}

func (s *Sequence) Continue() {
	job, ok := s.shift()
	if !ok {
		s.killCh <- s.key
		return
	}

	go func() {
		job.Action()
		s.Continue()
	}()
}

func (s *Sequence) shift() (job Job, ok bool) {
	s.m.Lock()
	defer s.m.Unlock()

	if len(s.jobs) == 0 {
		return Job{}, false
	}

	job, s.jobs = s.jobs[0], s.jobs[1:]

	return job, true
}
