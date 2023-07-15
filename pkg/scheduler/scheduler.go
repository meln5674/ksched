package scheduler

import (
	"fmt"

	"github.com/meln5674/ksched/pkg/object"
)

// A Scheduler can choose an executor for a object
type Scheduler[O object.Object] interface {
	// ObserveExecutor notifies the scheduler of an executor's status. A nil status indicates an executor has been removed.
	ObserveExecutor(name string, status *ExecutorStatus)
	// ObserveScheduled notifies the scheduler that a object has been scheduled to an executor
	ObserveScheduled(obj O)
	// ObserveCompleted notifies that a object has completed, successfully or otherwise
	ObserveCompleted(obj O)
	// ChooseExecutor determines the name of an executor to schedule a object to based on the current observed state
	ChooseExecutor(obj O) (string, error)
}

// UnschedulableError indicates a object cannot be scheduled for some reason, possibly caused by another error
type UnschedulableError struct {
	// Reason is a human-readable string indicating why an object cannot be scheduled
	Reason string
	// Cause is any error that interferred with the scheduling process
	Cause error
}

var _ = error(&UnschedulableError{})

// Error implements error
func (e *UnschedulableError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("Cannot schedule: %s", e.Reason)
	} else {
		return fmt.Sprintf("Cannot schedule: %s (caused by: %v)", e.Reason, e.Cause)
	}
}

var (
	// ErrNoExecutors is returned when a scheduler observes no executors
	ErrNoExecutors = &UnschedulableError{Reason: "No executors are known"}
	// ErrNoHealthyExecutors is returned when a scheduler observes executors, but none of them are schedulable
	ErrNoHealthyExecutors = &UnschedulableError{Reason: "No executors are healthy"}
)

// ExecutorStatus is the status of an executor as it pertains to scheduling
type ExecutorStatus string

const (
	// ExecutorHealthy indicates an executor is schedulable
	ExecutorHealthy ExecutorStatus = "Healthy"
	// ExecutorFull indicates an executor is in a state where it is schedulable but is voluntarily rejecting new objects
	ExecutorFull ExecutorStatus = "Full"
	// ExecutorHealthy indicates an executor is not schedulable
	ExecutorUnhealthy ExecutorStatus = "Unhealthy"
)

var (
	// ExecutorRemoved indicates an executor was removed
	ExecutorRemoved *ExecutorStatus = nil
)

// HavingStatus wraps an executor status so it can be passed to ObserveExecutor
func HavingStatus(status ExecutorStatus) *ExecutorStatus {
	return &status
}
