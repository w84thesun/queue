package queue

import (
	"testing"
)

func Test_findInsertIndex(t *testing.T) {
	type args struct {
		jobs     []Job
		priority int
	}
	tests := []struct {
		name    string
		args    args
		wantIdx int
	}{
		{
			name:    "empty",
			args:    args{jobs: testMakeJobs(), priority: 0},
			wantIdx: 0,
		},
		{
			name:    "head",
			args:    args{jobs: testMakeJobs(2, 3), priority: 1},
			wantIdx: 0,
		},
		{
			name:    "tail",
			args:    args{jobs: testMakeJobs(1, 2), priority: 3},
			wantIdx: 2,
		},
		{
			name:    "middle",
			args:    args{jobs: testMakeJobs(1, 3), priority: 2},
			wantIdx: 1,
		},
		{
			name:    "tail after same priority",
			args:    args{jobs: testMakeJobs(1, 2, 3), priority: 2},
			wantIdx: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx := findInsertIndex(tt.args.jobs, tt.args.priority)
			if gotIdx != tt.wantIdx {
				t.Errorf("findInsertIndex() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func testMakeJobs(priorities ...int) []Job {
	jobs := make([]Job, len(priorities))
	for i, p := range priorities {
		jobs[i] = Job{Priority: p}
	}
	return jobs
}
