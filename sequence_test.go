package queue

import (
	"strconv"
	"testing"
)

func Test_findInsertIndex(t *testing.T) {
	type args struct {
		jobs     []seqJob
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

func testMakeJobs(priorities ...int) []seqJob {
	jobs := make([]seqJob, len(priorities))
	for i, p := range priorities {
		jobs[i] = seqJob{priority: p}
	}
	return jobs
}

func BenchmarkSequence_Add(b *testing.B) {
	benches := []int{1, 5, 10, 25, 100, 500, 2000, 5000}

	for _, n := range benches {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkSequenceAddN(b, n)
		})
	}
}

func benchmarkSequenceAddN(b *testing.B, n int) {
	ch := make(chan string)
	go func() {
		for {
			<-ch
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		seq := NewSequence("key 1", ch)
		go seq.Run()

		for j := 0; j < n; j++ {
			seq.Add(0, "", func() {})
		}
	}
}
