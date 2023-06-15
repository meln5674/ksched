package supervisor

import (
	"context"
	"errors"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/go-logr/logr"

	"github.com/meln5674/ksched/pkg/object"
)

// A recordingScheduler records the last time an executor was observed
type recordingScheduler[O object.Object] struct {
	now                time.Time
	lastExecutorChange **time.Time
	scheduler          Scheduler[O]
}

// ObserveExecutor implements scheduler
func (r *recordingScheduler[O]) ObserveExecutor(name string, status *ExecutorStatus) {
	if *r.lastExecutorChange == nil {
		*r.lastExecutorChange = new(time.Time)
	}
	**r.lastExecutorChange = r.now
	r.scheduler.ObserveExecutor(name, status)
}

// ObserveScheduled implements scheduler
func (r *recordingScheduler[O]) ObserveScheduled(obj O) {
	r.scheduler.ObserveScheduled(obj)
}

// ObserveCompleted implements scheduler
func (r *recordingScheduler[O]) ObserveCompleted(obj O) {
	r.scheduler.ObserveCompleted(obj)
}

// ChooseExecutor implements scheduler
func (r *recordingScheduler[O]) ChooseExecutor(obj O) (string, error) {
	return r.scheduler.ChooseExecutor(obj)
}

// SchedulerState is the internal, mutable state of a scheduler
type SchedulerState[O object.Object] struct {
	scheduler          Scheduler[O]
	lock               sync.Mutex
	lastExecutorChange *time.Time
}

// LastExecutorChange returns the last time an executor's state was observed to change,
// or nil if no executors have been observed
func (s *SchedulerState[O]) LastExecutorChange() *time.Time {
	if s.lastExecutorChange == nil {
		return nil
	}
	t := new(time.Time)
	*t = *s.lastExecutorChange
	return t
}

// recordingScheduler returns a wrapper that will record if an executor is observed
func (s *SchedulerState[O]) recordingScheduler(now time.Time) Scheduler[O] {
	return &recordingScheduler[O]{
		scheduler:          s.scheduler,
		lastExecutorChange: &s.lastExecutorChange,
		now:                now,
	}
}

// NewSchedulerState returns a new scheduler state that will be maniuplated by the provided scheduler
func NewSchedulerState[O object.Object](s Scheduler[O]) *SchedulerState[O] {
	return &SchedulerState[O]{
		scheduler: s,
	}
}

// WithSchedulerLock executes a nil-ary function using a lock
func WithSchedulerLock0[O object.Object](s *SchedulerState[O], now time.Time, f func(s Scheduler[O])) {
	s.lock.Lock()
	defer s.lock.Unlock()
	f(s.recordingScheduler(now))
}

// WithSchedulerLock executes a 1-ary function using a lock
func WithSchedulerLock1[O object.Object, T any](s *SchedulerState[O], now time.Time, f func(s Scheduler[O]) T) T {
	s.lock.Lock()
	defer s.lock.Unlock()
	return f(s.recordingScheduler(now))
}

// WithSchedulerLock executes a 2-ary function using a lock
func WithSchedulerLock2[O object.Object, T any, U any](s *SchedulerState[O], now time.Time, f func(s Scheduler[O]) (T, U)) (T, U) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return f(s.recordingScheduler(now))
}

// SupervisorConfig is the static configuration of a supervisor.
// It should not change after a supervisor is started
type SupervisorConfig struct {
	// RequeueDelay is the period of time to wait before re-reconciling
	// an object if it needs a time-based retry
	RequeueDelay time.Duration
	// SchedulerJitter is the minimum period of time to wait before attempting
	// to schedule an object after an executor change is observed.
	// This can be used to prevent scheduling during, e.g. a rollout, or a series of failures.
	SchedulerJitter time.Duration
	// ArchiveTTL is the minimum time to wait before an object is marked as completed and
	// moving it to the archive
	ArchiveTTL time.Duration
	// Finalizer is the string to add to an objects finalizers to
	// indicate that it needs to be archived before it can be removed
	Finalizer string
}

// A Supervisor schedules objects to executors based on a scheduling algorithm
// TODO: Reassign if executor is removed
type Supervisor[O object.Object, OList client.ObjectList] struct {
	// Client is the Kubernetes client for the cluster containing objects
	Client client.Client
	// Scheme is the scheme for the Kubernetes cluster containing objects
	Scheme *runtime.Scheme
	// Log is the log to write messages to
	Log logr.Logger

	// NewObject must return a blank object of the type the supervisor supervises
	NewObject func() O
	// Archiver is used to move completed objects to an archive to reduce load on etcd
	Archiver[O, OList]
	// SchedulerState is the scheduling algorithm used to assign objects to executors,
	// as well as the state it acts on
	*SchedulerState[O]
	// SupervisorConfig is the static configuration
	SupervisorConfig
}

// Reconcile is a helper method that can be used to implement reconcile.Reconciler if no additional processing is needed
func (s *Supervisor[O, OList]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	now := time.Now()

	return WithSchedulerLock2[O, ctrl.Result, error](s.SchedulerState, now, func(sched Scheduler[O]) (ctrl.Result, error) {
		s.Log.Info("Got event")
		run := SupervisorRun[O, OList]{
			Supervisor: s,
			Scheduler:  sched,
			Obj:        s.NewObject(),
			Req:        req,
			Ctx:        ctx,
			Now:        now,
		}

		continu, res, err := run.Start()
		if !continu {
			return res, err
		}

		continu, res, err = run.HandleFinalizer()
		if !continu {
			return res, err
		}

		continu, res, err = run.CheckForCompletion()
		if !continu {
			return res, err
		}

		_, res, err = run.ScheduleIfReady()
		return res, err
	})
}

// BuilderForManager starts setting up the supervisor with the Manager.
// Use this function if you are wrapping Supervisor with your own type to customize reconciliation.
func (r *Supervisor[O, OList]) BuilderForManager(mgr ctrl.Manager) *builder.Builder {
	return ctrl.NewControllerManagedBy(mgr).
		For(r.NewObject())
}

// SetupWithManager sets up the the supervisor with the Manager.
// Use this function if you are using the supervisor as-is, with no customization.
func (r *Supervisor[O, OList]) SetupWithManager(mgr ctrl.Manager) error {
	return r.BuilderForManager(mgr).Complete(r)
}

// A SupervisorRun is all of the context for scheduling an object
type SupervisorRun[O object.Object, OList client.ObjectList] struct {
	// Supervisor is a pointer to the Supervisor for the run
	*Supervisor[O, OList]
	// Scheduler is the scheduling algorithm to use
	Scheduler[O]
	// req is the request that triggered the run
	Req ctrl.Request
	// ctx is the context of the request
	Ctx context.Context
	// obj is the object that triggered the request
	Obj O
	// now is the time at which the run started, and should be used for the current time for all operations
	Now time.Time

	Log logr.Logger
}

// Fetch fetches the object that triggered the run
func (r *SupervisorRun[O, OList]) Fetch() (bool, error) {
	err := r.Client.Get(r.Ctx, r.Req.NamespacedName, r.Obj)
	if err == nil {
		return true, nil
	}
	if client.IgnoreNotFound(err) == nil {
		return false, nil
	}
	return false, err
}

// EnsureFinalizer ensures that an object that is not being deleted has the finalizer set
func (r *SupervisorRun[O, OList]) EnsureFinalizer() (deleting bool, err error) {
	deleting = r.Obj.GetDeletionTimestamp() != nil
	if deleting {
		r.Log.Info("Deletion timestamp is set, not re-adding finalizer")
		return
	}
	r.Log.Info("Adding finalizer")
	if !controllerutil.AddFinalizer(r.Obj, r.Finalizer) {
		return
	}
	err = r.Client.Update(r.Ctx, r.Obj)
	return
}

// ArchiveAndFinalize adds the object to archive then removes its finalizer.
// The object is not deleted, the caller is responsible for deleting the object to
// prevent it from having the finalizer re-added on the next reconcile loop.
func (r *SupervisorRun[O, OList]) ArchiveAndFinalize() (bool, ctrl.Result, error) {
	err := r.ArchiveObject(r.Ctx, r.Obj)
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	r.Log.Info("Archived object, removing finalizer")
	if !controllerutil.RemoveFinalizer(r.Obj, r.Finalizer) {
		return false, ctrl.Result{}, nil
	}
	err = r.Client.Update(r.Ctx, r.Obj)
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	return true, ctrl.Result{}, nil
}

// HandleFinalizer ensures an object is being deleted is archived before removing its finalizer,
// and ensures an object not being deleted has the finalizer set
func (r *SupervisorRun[O, OList]) HandleFinalizer() (bool, ctrl.Result, error) {
	deleting, err := r.EnsureFinalizer()
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	if !deleting {
		return true, ctrl.Result{}, nil
	}
	r.Log.Info("Object is being deleted, archiving")
	return r.ArchiveAndFinalize()
}

// Start starts the run by attempting to fetch the object that triggered it,
// and checking if that object still exists.
func (r *SupervisorRun[O, OList]) Start() (bool, ctrl.Result, error) {
	exists, err := r.Fetch()
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	if !exists {
		r.Log.Info("Object was deleted")
		return false, ctrl.Result{}, nil
	}
	return true, ctrl.Result{}, nil
}

// CheckForCompletion checks if an object is scheduled and marked as completed, and updates the scheduler accordingly.
// If the object is also past the TTL, it is moved to the archive and deleted.
func (r *SupervisorRun[O, OList]) CheckForCompletion() (bool, ctrl.Result, error) {
	if r.Obj.AssignedTo() != "" {
		completedAt := r.Obj.CompletedAt()
		if completedAt == nil {
			r.Log.Info("Object is scheduled and in progress, checking back in later")
			return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, nil
		}

		r.ObserveCompleted(r.Obj)

		if r.Now.Sub(completedAt.Time) < r.ArchiveTTL {
			r.Log.Info("Object is completed, but has not passed archive TTL, checking back in later", "now", r.Now, "completedAt", completedAt.Time, "archiveAt", completedAt.Time.Add(r.ArchiveTTL))
			return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, nil
		}

		r.Log.Info("Object is completed and passed archive TTL, moving to archive", "now", r.Now, "completedAt", completedAt.Time, "archiveAt", completedAt.Time.Add(r.ArchiveTTL))
		continu, res, err := r.ArchiveAndFinalize()
		if !continu {
			return false, res, err
		}

		err = r.Client.Delete(r.Ctx, r.Obj)
		if err != nil {
			return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
		}
		r.Log.Info("Removed archived obj from k8s")
		return false, ctrl.Result{}, nil
	}
	return true, ctrl.Result{}, nil
}

// ScheduleIfReady checks if the scheduler is ready, and if so, uses it to assign the object to an executor.
func (r *SupervisorRun[O, OList]) ScheduleIfReady() (bool, ctrl.Result, error) {
	lastExecutorChange := r.LastExecutorChange()

	if lastExecutorChange == nil {
		r.Log.Info("No executor events have been observed yet, assuming during startup period, postponing scheduling")
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, nil
	}

	if r.Now.Sub(*lastExecutorChange) < r.SchedulerJitter {
		r.Log.Info(
			"Not enough time has passed since last executor change, postponing scheduling",
			"now", r.Now,
			"last", lastExecutorChange,
			"diff", r.Now.Sub(*lastExecutorChange),
		)
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, nil
	}

	executorName, err := r.ChooseExecutor(r.Obj)
	if errors.Is(err, ErrNoExecutors) || errors.Is(err, ErrNoHealthyExecutors) {
		r.Log.Info("No executors ready", "error", err)
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, nil
	}
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	r.Log.Info("Selected executor for object", "executor", executorName)

	r.Obj.AssignTo(executorName)
	err = r.Client.Update(r.Ctx, r.Obj)
	if err != nil {
		return false, ctrl.Result{Requeue: true, RequeueAfter: r.RequeueDelay}, err
	}
	r.ObserveScheduled(r.Obj)
	return true, ctrl.Result{}, nil
}

// NamespacePredicate filters events to those in a specific namespace
type NamespacePredicate struct {
	Namespace *string
}

// Create implements predicate.Predicate
func (p *NamespacePredicate) Create(e event.CreateEvent) bool {
	return p.Namespace == nil || e.Object.GetNamespace() == *p.Namespace
}

// Delete implements predicate.Predicate
func (p *NamespacePredicate) Delete(e event.DeleteEvent) bool {
	return p.Namespace == nil || e.Object.GetNamespace() == *p.Namespace
}

// Update implements predicate.Predicate
func (p *NamespacePredicate) Update(e event.UpdateEvent) bool {
	return p.Namespace == nil || e.ObjectNew.GetNamespace() == *p.Namespace
}

// Generic implements predicate.Predicate
func (p *NamespacePredicate) Generic(e event.GenericEvent) bool {
	return p.Namespace == nil || e.Object.GetNamespace() == *p.Namespace
}

// PodReady checks if all init containers and containers in a pod are marked as reday
func PodReady(pod *corev1.Pod) bool {
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, container := range pod.Status.InitContainerStatuses {
		if !container.Ready {
			return false
		}
	}
	for _, container := range pod.Status.ContainerStatuses {
		if !container.Ready {
			return false
		}
	}
	return true
}

// PodWatcherConfig is the static configuration for a PodWatcher
type PodWatcherConfig struct {
	// ExecutorSelector is the required set of labels (AND) for executor pods. If empty, all pods will be considered executors.
	ExecutorSelector metav1.LabelSelector
	// ExecutorNamespace is the namespace to restrict to. If empty, all namespaces will be considered for executors.
	ExecutorNamespace *string
	// RequeueDelay is how long to wait before checking an executor pod for its status again
	RequeueDelay time.Duration
}

// PodWatcher implements reconcile.Reconciler to watch for healthy pods, and provides their names to a scheduler as executors
// TODO: Handle namespaces
type PodWatcher[O object.Object] struct {
	// Client is the Kubernetes client to the cluster containing executor pods
	Client client.Client
	// Client is the scheme for the Kubernetes cluster containing executor pods
	Scheme *runtime.Scheme
	// Log is the log to write messages to
	Log logr.Logger

	// SchedulerState is the scheduler to provide pods as executors to
	*SchedulerState[O]
	// PodWatcherConfig is the static configuration
	PodWatcherConfig
}

// Reconcile implements reconcile.Reconciler
// +kubebuilder:rbac:groups=,resources=pods,verbs=get;list;watch
func (p *PodWatcher[O]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	now := time.Now()

	return WithSchedulerLock2[O, ctrl.Result, error](p.SchedulerState, now, func(s Scheduler[O]) (ctrl.Result, error) {

		p.Log.Info("Got pod event")
		run := PodWatcherRun[O]{
			PodWatcher: p,
			Scheduler:  s,
			pod:        &corev1.Pod{},
			req:        req,
			ctx:        ctx,
		}

		exists, err := run.Fetch()
		if err != nil {
			return ctrl.Result{Requeue: true, RequeueAfter: p.RequeueDelay}, nil
		}
		run.Observe(exists)
		return ctrl.Result{}, nil
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodWatcher[O]) SetupWithManager(mgr ctrl.Manager) error {
	labelPred, err := predicate.LabelSelectorPredicate(r.ExecutorSelector)
	if err != nil {
		return err
	}
	nsPred := &NamespacePredicate{Namespace: r.ExecutorNamespace}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(predicate.And(labelPred, nsPred)).
		Complete(r)
}

// PodWatcherRun is all of the context for observing a pod as an executor
type PodWatcherRun[O object.Object] struct {
	*PodWatcher[O]
	Scheduler[O]
	req ctrl.Request
	ctx context.Context
	pod *corev1.Pod
}

// Fetch fetches the pod that triggered the run
func (r *PodWatcherRun[O]) Fetch() (bool, error) {
	err := r.Client.Get(r.ctx, r.req.NamespacedName, r.pod)
	if err == nil {
		return true, nil
	}
	if client.IgnoreNotFound(err) == nil {
		r.pod.Name = r.req.Name
		r.pod.Namespace = r.req.Namespace
		return false, nil
	}
	return false, err
}

// Observe updates the schedule based on the existence and status of the pod that triggered the run
func (r *PodWatcherRun[O]) Observe(exists bool) {
	if !exists {
		r.Log.Info("Executor removed", "executor", r.pod.Name)
		r.ObserveExecutor(r.pod.Name, ExecutorRemoved)
		return
	}
	if PodReady(r.pod) {
		r.Log.Info("Executor healthy", "executor", r.pod.Name)
		r.ObserveExecutor(r.pod.Name, HavingStatus(ExecutorHealthy))
		return
	}
	r.Log.Info("Executor unhealthy", "executor", r.pod.Name)
	r.ObserveExecutor(r.pod.Name, HavingStatus(ExecutorUnhealthy))
}
