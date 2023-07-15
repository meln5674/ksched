package api_test

import (
	_ "embed"
	// . "github.com/onsi/ginkgo/v2"
	// . "github.com/onsi/gomega"
)

/*

var _ = Describe("Api", func() {

	var test testCase

	BeforeEach(func(ctx context.Context) {
		store.SBOM = make(map[string]report.SBOM, 0)
		store.VulnerabilityReport = make(map[string]report.VulnerabilityReport, 0)
		archive.Objects = make(map[string]map[string]*akv1a1.Scan, 0)
		test = testCase{}
	})

	JustBeforeEach(func() {
		test.validate()
	})

	runCase := func(ctx context.Context) {
		GinkgoHelper()
		test.test(api)
	}

	When("No token is present", func() {
		BeforeEach(func() {
			test.method = http.MethodGet
			test.path = "/prefix/"
			test.token = noToken()
			test.code = http.StatusUnauthorized
		})
		It("Should reject as unauthorized", func(ctx context.Context) { runCase(ctx) })
	})
	When("An invalid token is present", func() {
		BeforeEach(func() {
			test.method = http.MethodGet
			test.path = "/prefix/"
			test.token = invalidToken()
			test.code = http.StatusUnauthorized
		})
		It("Should reject as unauthorized", func(ctx context.Context) { runCase(ctx) })
	})
	When("A valid token is present", func() {
		BeforeEach(func() {
			test.token = validToken(map[string]interface{}{})
		})
		When("Creating a scan", func() {
			BeforeEach(func() {
				test.method = http.MethodPost
				test.body = bodyFor(scanNoReports.DeepCopy())
			})
			When("The namespace is blank", func() {
				BeforeEach(func() {
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/scans"
					test.code = http.StatusNotFound
				})
				It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
			})
			When("The namespace is set", func() {
				BeforeEach(func() {
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/namespaces/default/scans"
				})
				When("The user does not have permission to create a scan in that namespace", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to create a scan in that namespace", func() {
					BeforeEach(func() {
						test.policy = namespacedCreatePermissions
					})
					When("That scan already exists", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, scanNoReports.DeepCopy())
							test.code = http.StatusConflict
						})
						It("Should reject as a conflict", func(ctx context.Context) { runCase(ctx) })
					})
					When("That scan doesn't exist", func() {
						When("The metadata doesn't match", func() {
							BeforeEach(func() {
								test.policy = namespacedCreatePermissionsTwoNamespaces
								test.code = http.StatusBadRequest
								scan := scanNoReports.DeepCopy()
								scan.Namespace = "not-default"
								test.body = bodyFor(scan)
							})
							It("Should reject as a bad request", func(ctx context.Context) { runCase(ctx) })
						})
						When("The metadata matches", func() {
							BeforeEach(func() {
								test.code = http.StatusOK
							})
							It("Should succeed and be visible directly from kubernetes", func(ctx context.Context) {
								scan := scanNoReports.DeepCopy()
								runCase(ctx)
								expectObjectInK8s(ctx, new(akv1a1.Scan), scan)
								DeferCleanup(func(ctx context.Context) {
									Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, scan))).To(Succeed())
									Eventually(func() error { return k8sClient.Get(ctx, client.ObjectKeyFromObject(scan), scan) }, "5s").ShouldNot(Succeed())
									ctrl.Log.WithName("tests").Info("Removed from K8s", "type", scan.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(scan))
								})
							})
						})
					})
				})
			})
		})
		When("Listing scans", func() {
			BeforeEach(func() {
				test.method = http.MethodGet
			})
			When("The namespace is blank", func() {
				BeforeEach(func() {
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/scans"
				})
				When("The user does not have permission to list at cluster scope", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to list at cluster scope", func() {
					var scanSyftReportFilled, scanGrypeReportFilled, scanBothReportsFilled *akv1a1.Scan
					BeforeEach(func() {
						test.policy = clusterGetPermissions

						test.policy = clusterGetPermissions
						scanSyftReportFilled = setReports(scanSyftReport.DeepCopy(), "foo", testSyftReportBytes, nil)
						scanSyftReportFilled.Namespace = "default-1"
						scanGrypeReportFilled = setReports(scanGrypeReport.DeepCopy(), "foo", nil, testGrypeReportBytes)
						scanGrypeReportFilled.Namespace = "default-2"
						scanBothReportsFilled = setReports(scanBothReports.DeepCopy(), "foo", testSyftReportBytes, testGrypeReportBytes)
						scanBothReportsFilled.Namespace = "default-3"

						store.SBOM["syft-report-id"] = report.SBOM{SBOM: testSyftReport}
						store.VulnerabilityReport["grype-report-id"] = report.VulnerabilityReport{Report: testGrypeReport}
					})

					When("There are no scans in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), &akv1a1.ScanList{}, getListScans, []akv1a1.Scan{}, []akv1a1.Scan{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							var obj client.Object
							obj = scanNoReports.DeepCopy()
							k8sObject(ctx, obj)
							obj = scanSyftReport.DeepCopy()
							obj.SetNamespace("default-1")
							k8sObject(ctx, obj)
							obj = scanGrypeReport.DeepCopy()
							obj.SetNamespace("default-2")
							k8sObject(ctx, obj)
							obj = scanBothReports.DeepCopy()
							obj.SetNamespace("default-3")
							k8sObject(ctx, obj)
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans,
									[]akv1a1.Scan{
										*scanNoReports,
										*scanSyftReportFilled,
										*scanGrypeReportFilled,
										*scanBothReportsFilled,
									},
									[]akv1a1.Scan{},
								)
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in the archive, but not kubernetes", func() {
						BeforeEach(func(ctx context.Context) {

							var obj *akv1a1.Scan
							obj = scanNoReports.DeepCopy()
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							obj = scanSyftReport.DeepCopy()
							obj.SetNamespace("default-1")
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							obj = scanGrypeReport.DeepCopy()
							obj.SetNamespace("default-2")
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							obj = scanBothReports.DeepCopy()
							obj.SetNamespace("default-3")
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans,
									[]akv1a1.Scan{
										*scanNoReports,
										*scanSyftReportFilled,
										*scanGrypeReportFilled,
										*scanBothReportsFilled,
									},
									[]akv1a1.Scan{},
								)
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })

					})
					When("There are scans in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							var obj *akv1a1.Scan
							obj = scanNoReports.DeepCopy()
							k8sObject(ctx, obj)
							obj = scanSyftReport.DeepCopy()
							obj.SetNamespace("default-1")
							k8sObject(ctx, obj)
							obj = scanGrypeReport.DeepCopy()
							obj.SetNamespace("default-2")
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							obj = scanBothReports.DeepCopy()
							obj.SetNamespace("default-3")
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, obj)
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans,
									[]akv1a1.Scan{
										*scanNoReports,
										*scanSyftReportFilled,
										*scanGrypeReportFilled,
										*scanBothReportsFilled,
									},
									[]akv1a1.Scan{},
								)
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
				})
			})
			When("The namespace is set", func() {
				var scanSyftReportFilled, scanGrypeReportFilled, scanBothReportsFilled *akv1a1.Scan
				BeforeEach(func() {
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/namespaces/default/scans"
					scanSyftReportFilled = setReports(scanSyftReport.DeepCopy(), "foo", testSyftReportBytes, nil)
					scanGrypeReportFilled = setReports(scanGrypeReport.DeepCopy(), "foo", nil, testGrypeReportBytes)
					scanBothReportsFilled = setReports(scanBothReports.DeepCopy(), "foo", testSyftReportBytes, testGrypeReportBytes)

					store.SBOM["syft-report-id"] = report.SBOM{SBOM: testSyftReport}
					store.VulnerabilityReport["grype-report-id"] = report.VulnerabilityReport{Report: testGrypeReport}
				})

				When("The user does not have permission to list in any namespace", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user does has permission to list in another namespace namespace, but not that one", func() {
					BeforeEach(func() {
						test.policy = wrongNamespacedGetPermissions
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to list in that namespace", func() {
					BeforeEach(func() {
						test.policy = namespacedGetPermissions
						test.code = http.StatusOK
					})
					When("There are no scans in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{}, []akv1a1.Scan{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, scanNoReports.DeepCopy())
							k8sObject(ctx, scanSyftReport.DeepCopy())
							k8sObject(ctx, scanGrypeReport.DeepCopy())
							k8sObject(ctx, scanBothReports.DeepCopy())
							k8sObject(ctx, scanWrongNamespace.DeepCopy())
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in the archive, but not kubernetes", func() {
						BeforeEach(func(ctx context.Context) {
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanNoReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanSyftReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanGrypeReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanBothReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, scanNoReports.DeepCopy())
							k8sObject(ctx, scanSyftReport.DeepCopy())
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanGrypeReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanBothReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})

						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
				})
				When("The user has permission to list at the cluster scope", func() {
					BeforeEach(func() {
						test.policy = clusterGetPermissions
						test.code = http.StatusOK
					})
					When("There are no scans in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{}, []akv1a1.Scan{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, scanNoReports.DeepCopy())
							k8sObject(ctx, scanSyftReport.DeepCopy())
							k8sObject(ctx, scanGrypeReport.DeepCopy())
							k8sObject(ctx, scanBothReports.DeepCopy())
							k8sObject(ctx, scanWrongNamespace.DeepCopy())
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in the archive, but not kubernetes", func() {
						BeforeEach(func(ctx context.Context) {
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanNoReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanSyftReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanGrypeReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanBothReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are scans in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, scanNoReports.DeepCopy())
							k8sObject(ctx, scanSyftReport.DeepCopy())
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanGrypeReport)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanBothReports)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scanWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(akv1a1.Scan), new(akv1a1.ScanList), getListScans, []akv1a1.Scan{
									*scanNoReports,
									*scanSyftReportFilled,
									*scanGrypeReportFilled,
									*scanBothReportsFilled,
								}, []akv1a1.Scan{
									*scanWrongNamespace,
								})
							}
						})

						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
				})
			})
		})
		When("Getting a scan", func() {
			BeforeEach(func() {
				test.method = http.MethodGet
			})
			When("The namespace is blank", func() {
				BeforeEach(func(ctx context.Context) {
					k8sObject(ctx, scanNoReports.DeepCopy())
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/namespaces/scans/no-reports"
					test.policy = clusterGetPermissions
					test.code = http.StatusNotFound
				})
				It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
			})
			When("The namespace is set", func() {
				var scanSyftReportFilled, scanGrypeReportFilled, scanBothReportsFilled *akv1a1.Scan
				BeforeEach(func() {
					test.path = "/prefix/ankyra.meln5674.github.com/v1alpha1/namespaces/default/scans/test"
					scanSyftReportFilled = setReports(scanSyftReport.DeepCopy(), "foo", testSyftReportBytes, nil)
					scanSyftReportFilled.Name = "test"
					scanGrypeReportFilled = setReports(scanGrypeReport.DeepCopy(), "foo", nil, testGrypeReportBytes)
					scanGrypeReportFilled.Name = "test"
					scanBothReportsFilled = setReports(scanBothReports.DeepCopy(), "foo", testSyftReportBytes, testGrypeReportBytes)
					scanBothReportsFilled.Name = "test"

					store.SBOM["syft-report-id"] = report.SBOM{SBOM: testSyftReport}
					store.VulnerabilityReport["grype-report-id"] = report.VulnerabilityReport{Report: testGrypeReport}
				})

				When("The user does not have permission to get in any namespace", func() {
					BeforeEach(func() {
						test.policy = ""
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user does has permission to get in another namespace namespace, but not that one", func() {
					BeforeEach(func() {
						test.policy = wrongNamespacedGetPermissions
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to get in that namespace", func() {
					var scan *akv1a1.Scan
					BeforeEach(func() {
						test.policy = namespacedGetPermissions
					})
					When("The scan is not in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusNotFound
						})
						It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
					})
					When("There scan is in kubernetes but not the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						When("The scan has no reports", func() {
							BeforeEach(func(ctx context.Context) {
								ctrl.Log.WithName("tests").Info("expected", "expected", scanNoReports)
								scan = scanNoReports.DeepCopy()
								scan.Name = "test"
								k8sObject(ctx, scan)
								expected := scanNoReports.DeepCopy()
								expected.Name = scan.Name
								test.withBody = func(body io.Reader) {
									expectObject(body, expected)
								}
							})
							It("Should return it as-is", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has a syft report", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanSyftReport.DeepCopy()
								scan.Name = "test"
								k8sObject(ctx, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanSyftReportFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has a grype report", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanGrypeReport.DeepCopy()
								scan.Name = "test"
								k8sObject(ctx, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanGrypeReportFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has both reports", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanBothReports.DeepCopy()
								scan.Name = "test"
								k8sObject(ctx, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanBothReportsFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
					})
					When("The scans is in the archive but not in kubernetes", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						When("The scan has no reports", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanNoReports.DeepCopy()
								scan.Name = "test"
								archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scan)
								}
							})
							It("Should return it as-is", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has a syft report", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanSyftReport.DeepCopy()
								scan.Name = "test"
								archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanSyftReportFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has a grype report", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanGrypeReport.DeepCopy()
								scan.Name = "test"
								archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanGrypeReportFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
						When("The scan has both reports", func() {
							BeforeEach(func(ctx context.Context) {
								scan = scanBothReports.DeepCopy()
								scan.Name = "test"
								archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scan)
								test.withBody = func(body io.Reader) {
									expectObject(body, scanBothReportsFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
					})
					When("The scan is in both kubernetes and the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						BeforeEach(func(ctx context.Context) {
							scan = scanBothReports.DeepCopy()
							scan.Name = "test"
							scan2 := scanNoReports.DeepCopy()
							scan2.Name = "test"
							k8sObject(ctx, scan)
							archiveObject[*akv1a1.Scan, *akv1a1.ScanList](ctx, archive, scan2)
							test.withBody = func(body io.Reader) {
								expectObject(body, scanBothReportsFilled)
							}
						})
						It("Should return the scan from kubernetes", func(ctx context.Context) { runCase(ctx) })
					})
				})
			})
		})
	})
})

type testCase struct {
	method   string
	path     string
	body     io.Reader
	token    gotoken.TokenGetter
	policy   string
	code     int
	withBody func(io.Reader)
}

func (t testCase) validate() {
	GinkgoHelper()
	Expect(t.method).ToNot(BeZero())
	Expect(t.path).ToNot(BeZero())
	Expect(t.token).ToNot(BeZero())
	Expect(t.code).ToNot(BeZero())
}

func (t testCase) test(api *akapi.API) {
	GinkgoHelper()
	api.TokenGetter = t.token

	policy, err := template.New("test-policy").Parse(t.policy)
	Expect(err).ToNot(HaveOccurred())
	api.RBACPolicy = policy

	req, err := http.NewRequest(t.method, srv.URL+t.path, t.body)
	Expect(err).ToNot(HaveOccurred())
	ctrl.Log.WithName("request").Info(fmt.Sprintf("%s %s\n", req.Method, req.URL))
	resp, err := http.DefaultClient.Do(req)
	Expect(err).ToNot(HaveOccurred())
	Expect(resp).To(WithTransform(func(resp *http.Response) int { return resp.StatusCode }, Equal(t.code)))

	if t.withBody != nil {
		t.withBody(resp.Body)
	}
}

func noToken() gotoken.TokenGetter {
	return func(req *http.Request) (token *jwt.Token, present bool, err error) {
		return nil, false, nil
	}
}

func invalidToken() gotoken.TokenGetter {
	return func(req *http.Request) (token *jwt.Token, present bool, err error) {
		return nil, true, fmt.Errorf("This test token is invalid")
	}
}

func validToken(claims map[string]interface{}) gotoken.TokenGetter {
	return func(req *http.Request) (token *jwt.Token, present bool, err error) {
		return &jwt.Token{Claims: jwt.MapClaims(claims)}, true, nil
	}
}

func clearMetadata[O client.Object](obj O) O {
	obj.SetUID("")
	obj.SetResourceVersion("")
	obj.SetGeneration(0)
	obj.SetSelfLink("")
	obj.SetCreationTimestamp(metav1.Time{})
	obj.SetDeletionTimestamp(nil)
	obj.SetFinalizers(nil)
	obj.SetOwnerReferences(nil)
	obj.SetManagedFields(nil)
	return obj
}

func expectObjectInK8s[O client.Object](ctx context.Context, obj O, expected O) {
	GinkgoHelper()
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(expected), obj)).To(Succeed())
	obj.GetObjectKind().SetGroupVersionKind(expected.GetObjectKind().GroupVersionKind())
	Expect(clearMetadata(obj)).To(Equal(expected))
}

func expectObject[O client.Object](body io.Reader, expected O) {
	GinkgoHelper()
	buf := bytes.NewBuffer(make([]byte, 0))
	_, err := io.Copy(buf, body)
	Expect(err).ToNot(HaveOccurred())
	// ctrl.Log.WithName("tests").Info("Got object", "obj", string(buf.Bytes()))
	actual, err := runtime.Decode(api.Decoder, buf.Bytes())
	Expect(err).ToNot(HaveOccurred())
	gvks, _, err := testCluster.Environment.Scheme.ObjectKinds(actual)
	Expect(err).ToNot(HaveOccurred())
	actual.GetObjectKind().SetGroupVersionKind(gvks[0])
	Expect(clearMetadata(actual.(O))).To(Equal(clearMetadata(expected)))
}

func expectObjectList[O any, OPtr client.Object, OList client.ObjectList](body io.Reader, ptr OPtr, list OList, getItems func(OList) ([]O, []OPtr), objs []O, badObjs []O) {
	GinkgoHelper()
	buf := bytes.NewBuffer(make([]byte, 0))
	_, err := io.Copy(buf, body)
	Expect(err).ToNot(HaveOccurred())
	// ctrl.Log.WithName("tests").Info("Got objects", "objs", string(buf.Bytes()))
	actual, err := runtime.Decode(api.Decoder, buf.Bytes())
	Expect(err).ToNot(HaveOccurred())
	Expect(actual).To(BeAssignableToTypeOf(list))
	list = actual.(OList)
	items, ptrs := getItems(list)
	for _, ptr := range ptrs {
		clearMetadata(ptr)
	}
	// Expect((interface{}(objs[0])).(akv1a1.Scan).ObjectMeta).To(Equal((interface{}(items[0])).(akv1a1.Scan).ObjectMeta))
	Expect(items).To(ContainElements(objs))
	if len(badObjs) != 0 {
		for _, obj := range badObjs {
			Expect(items).ToNot(ContainElement(obj))
		}
	}
}

func getListScans(l *akv1a1.ScanList) ([]akv1a1.Scan, []*akv1a1.Scan) {
	ptrs := make([]*akv1a1.Scan, len(l.Items))
	for ix := range l.Items {
		ptrs[ix] = &l.Items[ix]
	}
	return l.Items, ptrs
}

var (
	namespacedCreatePermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [create]`
	namespacedCreatePermissionsTwoNamespaces = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [create]`
	namespacedGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [get, list]`
	wrongNamespacedGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: not-default
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [get, list]`
	clusterGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: test
rules:
- apiGroups: [ankyra.meln5674.github.com]
  resources: [scans]
  verbs: [get, list]`
)

func bodyFor(obj client.Object) io.Reader {
	GinkgoHelper()
	buf := bytes.NewBuffer(make([]byte, 0))
	Expect(json.NewEncoder(buf).Encode(obj)).To(Succeed())
	return buf
}

var (
	scanMeta = metav1.TypeMeta{
		APIVersion: akv1a1.GroupVersion.Group + "/" + akv1a1.GroupVersion.Version,
		Kind:       "Scan",
	}

	scanNoReports = &akv1a1.Scan{
		TypeMeta: scanMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "no-reports",
		},
		Spec: akv1a1.ScanSpec{
			Artifacts: []string{"foo"},
		},
	}
	scanWrongNamespace = &akv1a1.Scan{
		TypeMeta: scanMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "not-default",
			Name:      "no-reports",
		},
		Spec: akv1a1.ScanSpec{
			Artifacts: []string{"foo"},
		},
		Status: akv1a1.ScanStatus{
			Artifacts: map[string]akv1a1.ArtifactStatus{
				"foo": akv1a1.ArtifactStatus{},
			},
		},
	}
	scanSyftReport = &akv1a1.Scan{
		TypeMeta: scanMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "syft-report",
		},
		Spec: akv1a1.ScanSpec{
			Artifacts: []string{"foo"},
		},
		Status: akv1a1.ScanStatus{
			Artifacts: map[string]akv1a1.ArtifactStatus{
				"foo": akv1a1.ArtifactStatus{
					SyftReportID: newOf("syft-report-id"),
				},
			},
		},
	}
	scanGrypeReport = &akv1a1.Scan{
		TypeMeta: scanMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "grype-report",
		},
		Spec: akv1a1.ScanSpec{
			Artifacts: []string{"foo"},
		},
		Status: akv1a1.ScanStatus{
			Artifacts: map[string]akv1a1.ArtifactStatus{
				"foo": akv1a1.ArtifactStatus{
					GrypeReportID: newOf("grype-report-id"),
				},
			},
		},
	}
	scanBothReports = &akv1a1.Scan{
		TypeMeta: scanMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "both-reports",
		},
		Spec: akv1a1.ScanSpec{
			Artifacts: []string{"foo"},
		},
		Status: akv1a1.ScanStatus{
			Artifacts: map[string]akv1a1.ArtifactStatus{
				"foo": akv1a1.ArtifactStatus{
					SyftReportID:  newOf("syft-report-id"),
					GrypeReportID: newOf("grype-report-id"),
				},
			},
		},
	}
)

func newOf[T any](value T) *T {
	ptr := new(T)
	*ptr = value
	return ptr
}

func k8sObject[O client.Object](ctx context.Context, obj O) {
	GinkgoHelper()
	objStatus := obj.DeepCopyObject().(O)
	Expect(k8sClient.Create(ctx, obj)).To(Succeed())
	DeferCleanup(func(ctx context.Context) {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed())
		Eventually(func() error { return k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj) }, "5s").ShouldNot(Succeed())
		ctrl.Log.WithName("tests").Info("Removed from K8s", "type", obj.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(obj))
	})
	objStatus.SetResourceVersion(obj.GetResourceVersion())
	Expect(k8sClient.Status().Patch(ctx, objStatus, client.MergeFrom(obj))).To(Succeed())
	ctrl.Log.WithName("tests").Info("Added to k8s", "type", obj.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(obj))
}

func archiveObject[O kschedobj.Object, OList client.ObjectList](ctx context.Context, archive ksched.Archiver[O, OList], obj O) {
	GinkgoHelper()
	Expect(archive.ArchiveObject(ctx, obj)).To(Succeed())
	ctrl.Log.WithName("tests").Info("Added to Archive", "type", obj.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(obj))
}

func setReports(scan *akv1a1.Scan, artifact string, syft []byte, grype []byte) *akv1a1.Scan {
	GinkgoHelper()
	status := scan.Status.Artifacts[artifact]
	status.SyftReport = json.RawMessage(syft)
	status.GrypeReport = json.RawMessage(grype)
	scan.Status.Artifacts[artifact] = status
	return scan
}

//go:embed syft.json
var testSyftReportBytes []byte
var testSyftReport syft.Document

//go:embed grype.json
var testGrypeReportBytes []byte
var testGrypeReport grype.Document

*/
