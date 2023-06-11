package supervisor_test

import (
	"context"
	"errors"
	"time"

	"github.com/meln5674/gingk8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	example "github.com/meln5674/ksched/internal/testing/v1alpha1"
	"github.com/meln5674/ksched/pkg/supervisor"
)

var _ = Describe("Supervisor", Label("supervisor"), func() {

	type Supervisor = supervisor.Supervisor[*example.Example, *example.ExampleList]

	namespace := gingk8s.RandomNamespace{NeedFinalize: true}
	var podWatcher supervisor.PodWatcher[*example.Example]
	var sv supervisor.Supervisor[*example.Example, *example.ExampleList]
	var mgr ctrl.Manager
	var gk8s2 gingk8s.Gingk8s
	BeforeEach(func() {
		var err error
		gk8s2 = gk8s.ForSpec()

		gk8s2.ClusterAction(clusterID, "Random Namespace", &namespace)

		gctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		gk8s2.Setup(gctx)

		gingk8s.WithRandomPort[struct{}](func(port int) struct{} {
			mgr, err = ctrl.NewManager(cluster.Environment.Config, ctrl.Options{
				Scheme:                 cluster.Environment.Scheme,
				Port:                   port,
				Namespace:              namespace.Get(),
				MetricsBindAddress:     "0",
				HealthProbeBindAddress: "0",
			})
			Expect(err).ToNot(HaveOccurred())
			return struct{}{}
		})

		schedulerState := supervisor.NewSchedulerState[*example.Example](
			supervisor.NewRoundRobinScheduler[*example.Example](1),
		)

		sv = Supervisor{
			SupervisorConfig: supervisor.SupervisorConfig{
				SchedulerJitter: 1 * time.Second,
				ArchiveTTL:      5 * time.Minute,
				RequeueDelay:    30 * time.Second,
				Finalizer:       "supervisor-controller-test",
			},
			NewObject: func() *example.Example { return new(example.Example) },
			Log:       ctrl.Log.WithName("supervisor"),
			Archiver: &supervisor.InMemoryArchiver[*example.Example, *example.ExampleList]{
				Objects:   make(map[string]map[string]*example.Example),
				NewObject: func() *example.Example { return new(example.Example) },
				ListOf: func(es []*example.Example) *example.ExampleList {
					l := new(example.ExampleList)
					l.Items = make([]example.Example, len(es))
					for ix, e := range es {
						l.Items[ix] = *e
					}
					return l
				},
				DeepCopyInto: (*example.Example).DeepCopyInto,
				DeepCopy:     (*example.Example).DeepCopy,
			},
			SchedulerState: schedulerState,
			Client:         k8sClient,
			Scheme:         cluster.Environment.Scheme,
		}
		Expect(sv.SetupWithManager(mgr)).To(Succeed())

		ns := namespace.Get()

		podWatcher = supervisor.PodWatcher[*example.Example]{
			PodWatcherConfig: supervisor.PodWatcherConfig{
				ExecutorSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{"component": "executor"},
				},
				ExecutorNamespace: &ns,
			},
			Log:            ctrl.Log.WithName("pod-controller"),
			Scheme:         cluster.Environment.Scheme,
			Client:         k8sClient,
			SchedulerState: schedulerState,
		}
		Expect(podWatcher.SetupWithManager(mgr)).To(Succeed())

		mgrCtx, stopMgr := context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			err := mgr.Start(mgrCtx)
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			Expect(err).ToNot(HaveOccurred())
		}()
		DeferCleanup(stopMgr)
	})

	AfterEach(func() {

	})

	When("there are no executors", func() {
		It("should not schedule any objects", func(ctx context.Context) {

			By("Creating an object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
				},
			}

			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(k8sClient.Delete(ctx, o)).To(Succeed())
					// TODO: Figureo out why this never returns
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})

			By("Checking if the object is scheduled")
			Consistently(func() string {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				return o.AssignedTo()
			}, 5*sv.SchedulerJitter, "5s").Should(BeEmpty())
		})
	})

	When("there are no executors, but there were", func() {
		It("should not schedule any objects", func(ctx context.Context) {

			By("Creating an executor")

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "executor-",
					Namespace:    namespace.Get(),
					Labels:       podWatcher.ExecutorSelector.MatchLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "executor", Image: "rancher/pause:3.6"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			time.Sleep(2 * sv.SchedulerJitter)

			By("Setting the executor to become healthy")
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Ready: true,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

			By("Deleting the executor")

			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
			Eventually(func() error {
				pod2 := &corev1.Pod{}
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod2)
				ctrl.Log.Info("Pod still exists", "pod", pod2)
				return err
			}, "5m", "30s").ShouldNot(Succeed())

			time.Sleep(2 * sv.SchedulerJitter)

			By("Creating an object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
			}

			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(k8sClient.Delete(ctx, o)).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})

			By("Checking if the object was scheduled")

			Consistently(func() string {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				return o.AssignedTo()
			}, 5*sv.SchedulerJitter, "5s").Should(BeEmpty())
		})
	})

	When("there is an executor, but jitter has not passed", func() {
		It("should not schedule any objects", func(ctx context.Context) {

			sv.SchedulerJitter = 1 * time.Hour

			By("Creating an executor")

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "executor-",
					Namespace:    namespace.Get(),
					Labels:       podWatcher.ExecutorSelector.MatchLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "executor", Image: "rancher/pause:3.6"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			By("Setting the executor to become healthy")
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Ready: true,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

			DeferCleanup(func(ctx context.Context) {
				Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
				Eventually(func() error {
					pod2 := &corev1.Pod{}
					err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod2)
					ctrl.Log.Info("Pod still exists", "pod", pod2)
					return err
				}, "5m", "30s").ShouldNot(Succeed())
			})

			time.Sleep(5 * time.Second)

			By("Creating an object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
			}

			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(k8sClient.Delete(ctx, o)).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})

			By("Checking if the object was scheduled")

			Consistently(func() string {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				return o.AssignedTo()
			}, "15s", "5s").Should(BeEmpty())
		})
	})
	When("there is an executor", func() {
		It("should schedule to it", func(ctx context.Context) {

			By("Creating an executor")

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "executor-",
					Namespace:    namespace.Get(),
					Labels:       podWatcher.ExecutorSelector.MatchLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "executor", Image: "rancher/pause:3.6"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			time.Sleep(2 * sv.SchedulerJitter)

			By("Setting the executor to become healthy")
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Ready: true,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

			time.Sleep(2 * sv.SchedulerJitter)

			By("Creating an object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
			}

			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(k8sClient.Delete(ctx, o)).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})

			By("Confirming the object is scheduled")

			Eventually(func() string {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				return o.AssignedTo()
			}, "30s", "5s").Should(Equal(pod.Name))
		})
	})

	When("there is an object in progress", func() {
		It("should not delete it", func(ctx context.Context) {
			sv.ArchiveTTL = 1 * time.Second
			By("Creating a completed object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
			}
			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(k8sClient.Delete(ctx, o)).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})

			time.Sleep(2 * sv.ArchiveTTL)

			By("Confirming the object isn't in the archive")

			o2 := &example.Example{}
			Expect(sv.Archiver.GetObject(ctx, client.ObjectKeyFromObject(o), o2)).ToNot(Succeed())

			By("Confirming the pipline is still in k8s")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o2)).To(Succeed())
		})
	})

	When("there is a completed object within the archive TTL", func() {
		It("should not delete it", func(ctx context.Context) {
			sv.ArchiveTTL = 1 * time.Second
			By("Creating a completed object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
			}
			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, o))).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})
			now := metav1.Now()
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				o.Complete(now, true)
				return k8sClient.Status().Update(ctx, o)
			}).Should(Succeed())

			By("Confirming the object isn't in the archive")

			o2 := &example.Example{}
			Expect(sv.Archiver.GetObject(ctx, client.ObjectKeyFromObject(o), o2)).ToNot(Succeed())

			By("Confirming the object is still in k8s")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o2)).To(Succeed())
		})
	})

	When("there is a completed object past the archive TTL", func() {
		It("should archive and delete it", func(ctx context.Context) {
			sv.ArchiveTTL = 1 * time.Second
			By("Creating a completed object")

			o := &example.Example{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dummy",
					Namespace: namespace.Get(),
					Labels: map[string]string{
						"dummy": "dummy",
					},
				},
				Spec: example.ExampleSpec{
					AssignedTo: "foo",
				},
			}
			Expect(k8sClient.Create(ctx, o)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				if !gk8s2.GetOptions().NoSpecCleanup {
					By("Deleting the object")
					Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, o))).To(Succeed())
					Eventually(func() error {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)
						ctrl.Log.Info("Object still exists", "object", o)
						return err
					}, "5m", "30s").ShouldNot(Succeed())
				}
			})
			now := metav1.Now()
			time.Sleep(2 * sv.ArchiveTTL)
			Eventually(func() error {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o)).To(Succeed())
				o.Complete(now, true)
				return k8sClient.Status().Update(ctx, o)
			}).Should(Succeed())

			By("Confirming the pipline is no longer k8s")
			Eventually(func() error {
				o2 := &example.Example{}
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(o), o2)
				ctrl.Log.Info("Object still exists", "object", o2)
				return err
			}, "5m", "30s").ShouldNot(Succeed())

			By("Confirming the object in the archive")
			o2 := &example.Example{}
			Expect(sv.Archiver.GetObject(ctx, client.ObjectKeyFromObject(o), o2)).To(Succeed())
			Expect(o2).To(Equal(o))
		})
	})
})
