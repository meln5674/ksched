package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	ev1a1 "github.com/meln5674/ksched/internal/testing/v1alpha1"
	ksched "github.com/meln5674/ksched/pkg/api"
	"github.com/meln5674/ksched/pkg/archive"

	"github.com/meln5674/gingk8s"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API Suite")
}

var _ = BeforeSuite(func() {
	var err error

	Expect(json.Unmarshal(mutatedDataBytes, &mutatedData)).To(Succeed())
	mutatedDataBytes, err = json.Marshal(&mutatedData)
	Expect(err).NotTo(HaveOccurred())

	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	gk8s := gingk8s.ForSuite(ginkgo.GinkgoT())

	clusterID := gk8s.Cluster(&testCluster)
	gk8s.Manifests(clusterID, &gingk8s.KubernetesManifests{
		ResourceObjects: []interface{}{
			corev1.Namespace{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "not-default",
				},
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	DeferCleanup(cancel)
	gk8s.Setup(ctx)

	k8sClient, err = client.New(testCluster.Environment.Config, client.Options{})
	Expect(err).NotTo(HaveOccurred())

	err = corev1.AddToScheme(testCluster.Environment.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = rbacv1.AddToScheme(testCluster.Environment.Scheme)
	Expect(err).NotTo(HaveOccurred())
	err = ev1a1.AddToScheme(testCluster.Environment.Scheme)
	Expect(err).NotTo(HaveOccurred())

	api.K8s = k8sClient
	api.Scheme = testCluster.Environment.Scheme
	api.Decoder = serializer.NewCodecFactory(testCluster.Environment.Scheme).UniversalDeserializer()
	Expect(ksched.RegisterType(api, "examples", new(ev1a1.Example), new(ev1a1.ExampleList), exampleInfo)).To(Succeed())

	srv = httptest.NewServer(api)
	DeferCleanup(srv.Close)

	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-1",
		},
	})).To(Succeed())
	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-2",
		},
	})).To(Succeed())
	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-3",
		},
	})).To(Succeed())
})

var (
	testCluster = gingk8s.EnvTestCluster{
		Environment: envtest.Environment{
			CRDDirectoryPaths: []string{"../../internal/testing/outputs/"},
		},
	}
	k8sClient client.Client

	exampleArchive = &archive.InMemoryArchiver[*ev1a1.Example, *ev1a1.ExampleList]{
		Objects:   make(map[string]map[string]*ev1a1.Example, 0),
		NewObject: func() *ev1a1.Example { return new(ev1a1.Example) },
		ListOf: func(scans []*ev1a1.Example) *ev1a1.ExampleList {
			list := new(ev1a1.ExampleList)
			list.Items = make([]ev1a1.Example, len(scans))
			for ix, scan := range scans {
				list.Items[ix] = *scan
			}
			return list
		},
		DeepCopyInto: func(src, dest *ev1a1.Example) { src.DeepCopyInto(dest) },
		DeepCopy:     func(scan *ev1a1.Example) *ev1a1.Example { return scan.DeepCopy() },
	}
	api = &ksched.API{
		Config: ksched.Config{
			Prefix: "/prefix/",
		},
		Log: ctrl.Log.WithName("api"),
	}
	exampleInfo ksched.ObjectInfo[*ev1a1.Example, *ev1a1.ExampleList] = &exampleObjectInfo{archive: exampleArchive}

	srv *httptest.Server
)

type exampleObjectInfo struct {
	archive archive.Archiver[*ev1a1.Example, *ev1a1.ExampleList]
}

var _ = ksched.ObjectInfo[*ev1a1.Example, *ev1a1.ExampleList]((*exampleObjectInfo)(nil))

func (e *exampleObjectInfo) New() *ev1a1.Example {
	return new(ev1a1.Example)
}
func (e *exampleObjectInfo) NewList() *ev1a1.ExampleList {
	return new(ev1a1.ExampleList)
}

func (e *exampleObjectInfo) MutateFromRead(ctx context.Context, obj *ev1a1.Example) error {
	if obj.Spec.HasData {
		obj.Status.MutatedData = &mutatedData
	}
	return nil
}
func (e *exampleObjectInfo) MutateFromList(ctx context.Context, objs *ev1a1.ExampleList) error {
	for ix := range objs.Items {
		if objs.Items[ix].Spec.HasData {
			objs.Items[ix].Status.MutatedData = &mutatedData
		}
	}
	return nil
}
func (e *exampleObjectInfo) MutateForWrite(ctx context.Context, obj *ev1a1.Example) error {
	obj.Status.MutatedData = nil
	return nil
}

func (e *exampleObjectInfo) Namespaced() bool {
	return true
}

func (e *exampleObjectInfo) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   ev1a1.GroupVersion.Group,
		Version: ev1a1.GroupVersion.Version,
		Kind:    "Example",
	}
}
func (e *exampleObjectInfo) ListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   ev1a1.GroupVersion.Group,
		Version: ev1a1.GroupVersion.Version,
		Kind:    "ExampleList",
	}
}
func (e *exampleObjectInfo) Archive() archive.Archiver[*ev1a1.Example, *ev1a1.ExampleList] {
	return e.archive
}
