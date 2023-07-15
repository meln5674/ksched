package scheduler_test

import (
	"context"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/meln5674/gingk8s"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	example "github.com/meln5674/ksched/internal/testing/v1alpha1"
)

func TestScheduler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Supervisor Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	gk8s = gingk8s.ForSuite(GinkgoT())

	Expect(example.AddToScheme(scheme.Scheme)).To(Succeed())

	// cluster.Environment.ControlPlane.APIServer.Configure().Append("v", "10")
	clusterID = gk8s.Cluster(&cluster)

	ctx, cancel := context.WithCancel(context.Background())
	DeferCleanup(cancel)
	gk8s.Setup(ctx)

	var err error
	k8sClient, err = client.New(cluster.Environment.Config, client.Options{Scheme: scheme.Scheme})
	Expect(err).ToNot(HaveOccurred())
})

var (
	gk8s      gingk8s.Gingk8s
	clusterID gingk8s.ClusterID
	cluster   = gingk8s.EnvTestCluster{
		Environment: envtest.Environment{
			CRDDirectoryPaths: []string{"../../internal/testing/outputs/"},
			ControlPlane: envtest.ControlPlane{
				APIServer: &envtest.APIServer{
					Out: GinkgoWriter,
					Err: GinkgoWriter,
				},
				Etcd: &envtest.Etcd{
					Out: GinkgoWriter,
					Err: GinkgoWriter,
				},
			},
		},
	}

	k8sClient client.Client
)
