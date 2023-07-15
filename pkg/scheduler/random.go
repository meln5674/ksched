package scheduler

import (
	"math/rand"

	"github.com/meln5674/ksched/pkg/object"
)

// A RandomScheduler maintains a circular list of healthy executors and just schedules to each executor in order without consideration for load, etc
type RandomScheduler[O object.Object] struct {
	known   map[string]struct{}
	healthy map[string]struct{}
}

// var _ = Scheduler(&RandomScheduler{})

// NewRandomScheduler creates a new round-robin scheduler with an initial capacity of expected executors
func NewRandomScheduler[O object.Object](initialCapacity int) *RandomScheduler[O] {
	return &RandomScheduler[O]{
		known: make(map[string]struct{}, initialCapacity),
	}
}

// ObserveExecutor implements Scheduler
func (s *RandomScheduler[O]) ObserveExecutor(name string, status *ExecutorStatus) {
	// Regardless of health record known executors to determine which error to use
	if status == nil {
		delete(s.known, name)
	} else {
		s.known[name] = struct{}{}
	}
	if *status == ExecutorHealthy {
		s.healthy[name] = struct{}{}
	} else {
		delete(s.healthy, name)
	}
}

// ObserveScheduled implements Scheduler
func (s *RandomScheduler[O]) ObserveScheduled(obj O) {
	// Don't care
}

// ObserveCompleted implements Scheduler
func (s *RandomScheduler[O]) ObserveCompleted(obj O) {
	// Don't care
}

// ChooseExecutor implements Scheduler
func (s *RandomScheduler[O]) ChooseExecutor(obj O) (string, error) {
	if len(s.known) == 0 {
		return "", ErrNoExecutors
	}

	if len(s.healthy) == 0 {
		return "", ErrNoHealthyExecutors
	}

	// TODO: This is really inefficient, but this isn't a serious algorythm anyways, but we should fix it anyways
	chosenIx := rand.Intn(len(s.healthy))
	ix := 0
	for name := range s.healthy {
		if ix == chosenIx {
			return name, nil
		}
		ix++
	}
	panic("This should never happen")
}
