package supervisor

import (
	"github.com/meln5674/ksched/pkg/object"
)

// A RoundRobinScheduler maintains a circular list of healthy executors and just schedules to each executor in order without consideration for load, etc
type RoundRobinScheduler[O object.Object] struct {
	// head is the firt node in the circular doubly linked-list
	head *linkedListNode
	// tail is the firt node in the circular doubly linked-list
	tail *linkedListNode
	// next is the next executor to schedule to
	next *linkedListNode
	// shortcuts is a map from name to location in the list
	shortcuts map[string]*linkedListNode
	// known are all known executors, healthy or not
	known map[string]struct{}
}

// linkedListNode is a node in a doubly linked-list
type linkedListNode struct {
	next *linkedListNode
	prev *linkedListNode
	name string
}

// var _ = Scheduler(&RoundRobinScheduler{})

// NewRoundRobinScheduler creates a new round-robin scheduler with an initial capacity of expected executors
func NewRoundRobinScheduler[O object.Object](initialCapacity int) *RoundRobinScheduler[O] {
	return &RoundRobinScheduler[O]{
		head:      nil,
		tail:      nil,
		next:      nil,
		shortcuts: make(map[string]*linkedListNode, initialCapacity),
		known:     make(map[string]struct{}, initialCapacity),
	}
}

// ObserveExecutor implements Scheduler
func (s *RoundRobinScheduler[O]) ObserveExecutor(name string, status *ExecutorStatus) {
	// Regardless of health record known executors to determine which error to use
	if status == nil {
		delete(s.known, name)
	} else {
		s.known[name] = struct{}{}
	}
	// Empty List
	if s.head == nil {
		// Unhealthy: No-op
		if status == nil || *status != ExecutorHealthy {
			return
		}
		// Healthy: Set as only node, link to self
		node := &linkedListNode{
			name: name,
		}
		node.next = node
		node.prev = node
		s.head = node
		s.tail = node
		s.next = node
		s.shortcuts[name] = node
		return
	}
	// Healthy, Non-empty list
	if status != nil && *status == ExecutorHealthy {
		_, ok := s.shortcuts[name]
		// Already in list: No-op
		if ok {
			return
		}
		// Not in list: Insert between tail and head
		node := &linkedListNode{
			name: name,
			prev: s.tail,
			next: s.tail.next,
		}
		node.prev.next = node
		node.next.prev = node
		s.tail = node
		s.shortcuts[name] = node
		return
	}
	// Unhealthy, Non-empty list
	node, ok := s.shortcuts[name]
	// Not in list: No-op
	if !ok {
		return
	}
	// In list: Remove shortcut
	delete(s.shortcuts, name)
	// List is singleton: Revert to empty list
	if node.prev == node {
		s.head = nil
		s.tail = nil
		s.next = nil
		node.next = nil
		node.prev = nil
		return
	}
	// List is not singleton: Link predecessor to successor
	node.next.prev = node.prev
	node.prev.next = node.next
	// If node would have been next, skip to next
	if s.next == node {
		s.next = node.next
	}
}

// ObserveScheduled implements Scheduler
func (s *RoundRobinScheduler[O]) ObserveScheduled(obj O) {
	// Don't care
}

// ObserveCompleted implements Scheduler
func (s *RoundRobinScheduler[O]) ObserveCompleted(obj O) {
	// Don't care
}

// ChooseExecutor implements Scheduler
func (s *RoundRobinScheduler[O]) ChooseExecutor(obj O) (string, error) {
	if s.next != nil {
		name := s.next.name
		s.next = s.next.next
		return name, nil
	}

	if len(s.known) == 0 {
		return "", ErrNoExecutors
	}

	return "", ErrNoHealthyExecutors
}
