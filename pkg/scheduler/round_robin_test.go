package scheduler_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	example "github.com/meln5674/ksched/internal/testing/v1alpha1"
	"github.com/meln5674/ksched/pkg/scheduler"
)

var _ = Describe("RoundRobinScheduler[*example.Example]", Label("scheduler"), func() {
	var sched *scheduler.RoundRobinScheduler[*example.Example]
	BeforeEach(func() {
		sched = scheduler.NewRoundRobinScheduler[*example.Example](10)
	})
	When("it's empty", func() {

		It("Should fail to schedule", func() {
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).To(Equal(scheduler.ErrNoExecutors))
			Expect(name).To(BeEmpty())
		})
	})

	When("it has a single executor", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorHealthy))
		})

		It("Should only schedule that executor", func() {
			Consistently(func() string {
				name, err := sched.ChooseExecutor(&example.Example{})
				Expect(err).ToNot(HaveOccurred())
				return name
			}, "1s", "100ms").Should(Equal("test"))
		})
	})

	When("it has a single executor that is removed", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test", scheduler.ExecutorRemoved)
		})

		It("Should fail to schedule", func() {
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).To(Equal(scheduler.ErrNoExecutors))
			Expect(name).To(BeEmpty())
		})
	})

	When("it has a single executor that is unhealthy", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))
		})

		It("Should fail to schedule", func() {
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).To(Equal(scheduler.ErrNoHealthyExecutors))
			Expect(name).To(BeEmpty())
		})
	})

	When("it has a single executor that becomes unhealthy", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorHealthy))
		})

		It("Should schedule then fail to schedule", func() {
			By("Scheduling while healthy")
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test"))

			By("Marking the executor as unhealthy")
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))

			By("Failing to schedule")
			name, err = sched.ChooseExecutor(&example.Example{})
			Expect(err).To(Equal(scheduler.ErrNoHealthyExecutors))
			Expect(name).To(BeEmpty())
		})
	})

	When("it has a single executor that is removed", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test", scheduler.HavingStatus(scheduler.ExecutorHealthy))
		})

		It("Should schedule then fail to schedule", func() {
			By("Scheduling while healthy")
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test"))

			By("Removing the executor")
			sched.ObserveExecutor("test", scheduler.ExecutorRemoved)

			By("Failing to schedule")
			name, err = sched.ChooseExecutor(&example.Example{})
			Expect(err).To(Equal(scheduler.ErrNoExecutors))
			Expect(name).To(BeEmpty())
		})
	})

	When("it would have scheduled to an unhealthy executor next", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test-1", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-2", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-3", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test-1"))
			sched.ObserveExecutor("test-2", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))
		})

		It("should should schedule to the next executor instead", func() {
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test-3"))
		})
	})

	When("it would have scheduled to a removed executor next", func() {
		BeforeEach(func() {
			sched.ObserveExecutor("test-1", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-2", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-3", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test-1"))
			sched.ObserveExecutor("test-2", scheduler.ExecutorRemoved)
		})

		It("should should schedule to the next executor instead", func() {
			name, err := sched.ChooseExecutor(&example.Example{})
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("test-3"))
		})
	})

	When("it has a multiple healthy, unhealthy, and removed executors", func() {
		healthy := []interface{}{"test-1", "test-3", "test-6"}
		unhealthy := []interface{}{"test-2", "test-5", "test-9"}
		removed := []interface{}{"test-4", "test-7", "test-8"}
		BeforeEach(func() {
			sched.ObserveExecutor("test-1", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-2", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))
			sched.ObserveExecutor("test-3", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-4", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-5", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))
			sched.ObserveExecutor("test-6", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-7", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))
			sched.ObserveExecutor("test-8", scheduler.HavingStatus(scheduler.ExecutorHealthy))
			sched.ObserveExecutor("test-4", scheduler.ExecutorRemoved)
			sched.ObserveExecutor("test-9", scheduler.HavingStatus(scheduler.ExecutorUnhealthy))

			sched.ObserveExecutor("test-7", scheduler.ExecutorRemoved)
			sched.ObserveExecutor("test-8", scheduler.ExecutorRemoved)
		})

		It("Should schedule to all healthy executors at some point", func() {
			scheduled := make(map[string]struct{}, len(healthy))
			Consistently(func() string {
				name, err := sched.ChooseExecutor(&example.Example{})
				Expect(err).ToNot(HaveOccurred())
				scheduled[name] = struct{}{}
				return name
			}, "1s", "100ms").Should(BeElementOf(healthy...))
			allScheduled := make([]string, len(scheduled))
			for name := range scheduled {
				allScheduled = append(allScheduled, name)
			}
			Expect(allScheduled).To(ContainElements(healthy...))
		})

		It("Should not schedule to unhealthy executors", func() {
			Consistently(func() string {
				name, err := sched.ChooseExecutor(&example.Example{})
				Expect(err).ToNot(HaveOccurred())
				return name
			}, "1s", "100ms").ShouldNot(BeElementOf(unhealthy...))
		})

		It("Should not schedule to removed executors", func() {
			Consistently(func() string {
				name, err := sched.ChooseExecutor(&example.Example{})
				Expect(err).ToNot(HaveOccurred())
				return name
			}, "1s", "100ms").ShouldNot(BeElementOf(removed...))
		})
	})
})
