package api_test

// . "github.com/onsi/ginkgo/v2"
// . "github.com/onsi/gomega"

/*

func TestApi(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Api Suite")
}

var _ = BeforeSuite(func() {
	var err error

	Expect(json.Unmarshal(testSyftReportBytes, &testSyftReport)).To(Succeed())
	testSyftReportBytes, err = json.Marshal(&testSyftReport)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(testGrypeReportBytes, &testGrypeReport)).To(Succeed())
	testGrypeReportBytes, err = json.Marshal(&testGrypeReport)
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
	err = akv1a1.AddToScheme(testCluster.Environment.Scheme)
	Expect(err).NotTo(HaveOccurred())

	api.K8s = k8sClient
	api.Scheme = testCluster.Environment.Scheme
	api.Decoder = serializer.NewCodecFactory(testCluster.Environment.Scheme).UniversalDeserializer()

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
			CRDDirectoryPaths: []string{"../../config/crd/bases"},
		},
	}
	k8sClient client.Client

	store = &report.InMemoryStore{
		SBOM:                make(map[string]report.SBOM, 0),
		VulnerabilityReport: make(map[string]report.VulnerabilityReport, 0),
	}
	archive = &ksched.InMemoryArchiver[*akv1a1.Scan, *akv1a1.ScanList]{
		Objects:   make(map[string]map[string]*akv1a1.Scan, 0),
		NewObject: func() *akv1a1.Scan { return new(akv1a1.Scan) },
		ListOf: func(scans []*akv1a1.Scan) *akv1a1.ScanList {
			list := new(akv1a1.ScanList)
			list.Items = make([]akv1a1.Scan, len(scans))
			for ix, scan := range scans {
				list.Items[ix] = *scan
			}
			return list
		},
		DeepCopyInto: func(src, dest *akv1a1.Scan) { src.DeepCopyInto(dest) },
		DeepCopy:     func(scan *akv1a1.Scan) *akv1a1.Scan { return scan.DeepCopy() },
	}
	api = &akapi.API{
		Config: akapi.Config{
			Prefix: "/prefix/",
		},
		Store:   store,
		Archive: archive,
		Log:     ctrl.Log.WithName("api"),
	}

	srv *httptest.Server
)

*/
