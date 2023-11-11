package api_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"text/template"

	"github.com/golang-jwt/jwt/v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/meln5674/gotoken"

	ev1a1 "github.com/meln5674/ksched/internal/testing/v1alpha1"
	ksched "github.com/meln5674/ksched/pkg/api"
	"github.com/meln5674/ksched/pkg/archive"
	"github.com/meln5674/ksched/pkg/object"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Api", func() {

	var test testCase

	BeforeEach(func(ctx context.Context) {
		exampleArchive.Objects = make(map[string]map[string]*ev1a1.Example, 0)
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
			test.path = "/prefix/apis/"
			test.token = noToken()
			test.code = http.StatusUnauthorized
		})
		It("Should reject as unauthorized", func(ctx context.Context) { runCase(ctx) })
	})
	When("An invalid token is present", func() {
		BeforeEach(func() {
			test.method = http.MethodGet
			test.path = "/prefix/apis/"
			test.token = invalidToken()
			test.code = http.StatusUnauthorized
		})
		It("Should reject as unauthorized", func(ctx context.Context) { runCase(ctx) })
	})
	When("A valid token is present", func() {
		BeforeEach(func() {
			test.token = validToken(map[string]interface{}{})
		})
		When("Creating a example", func() {
			BeforeEach(func() {
				test.method = http.MethodPost
				test.body = bodyFor(exampleNoData.DeepCopy())
			})
			When("The namespace is blank", func() {
				BeforeEach(func() {
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/examples"
					test.code = http.StatusNotFound
				})
				It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
			})
			When("The namespace is set", func() {
				BeforeEach(func() {
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/namespaces/default/examples"
				})
				When("The user does not have permission to create a example in that namespace", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to create a example in that namespace", func() {
					BeforeEach(func() {
						test.policy = namespacedCreatePermissions
					})
					When("That example already exists", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, exampleNoData.DeepCopy())
							test.code = http.StatusConflict
						})
						It("Should reject as a conflict", func(ctx context.Context) { runCase(ctx) })
					})
					When("That example doesn't exist", func() {
						When("The metadata doesn't match", func() {
							BeforeEach(func() {
								test.policy = namespacedCreatePermissionsTwoNamespaces
								test.code = http.StatusBadRequest
								example := exampleNoData.DeepCopy()
								example.Namespace = "not-default"
								test.body = bodyFor(example)
							})
							It("Should reject as a bad request", func(ctx context.Context) { runCase(ctx) })
						})
						When("The metadata matches", func() {
							BeforeEach(func() {
								test.code = http.StatusOK
							})
							It("Should succeed and be visible directly from kubernetes", func(ctx context.Context) {
								example := exampleNoData.DeepCopy()
								runCase(ctx)
								expectObjectInK8s(ctx, new(ev1a1.Example), example)
								DeferCleanup(func(ctx context.Context) {
									Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, example))).To(Succeed())
									Eventually(func() error { return k8sClient.Get(ctx, client.ObjectKeyFromObject(example), example) }, "5s").ShouldNot(Succeed())
									ctrl.Log.WithName("tests").Info("Removed from K8s", "type", example.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(example))
								})
							})
						})
					})
				})
			})
		})
		When("Listing examples", func() {
			BeforeEach(func() {
				test.method = http.MethodGet
			})
			When("The namespace is blank", func() {
				BeforeEach(func() {
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/examples"
				})
				When("The user does not have permission to list at cluster scope", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to list at cluster scope", func() {
					var exampleWithDataFilled *ev1a1.Example
					BeforeEach(func() {
						test.policy = clusterGetPermissions

						test.policy = clusterGetPermissions
						exampleWithDataFilled = setMutatedData(exampleWithData.DeepCopy(), mutatedData)
						exampleWithDataFilled.Namespace = "default-1"
					})

					When("There are no examples in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), &ev1a1.ExampleList{}, getListScans, []ev1a1.Example{}, []ev1a1.Example{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							var obj client.Object
							obj = exampleNoData.DeepCopy()
							k8sObject(ctx, obj)
							obj = exampleWithData.DeepCopy()
							obj.SetNamespace("default-1")
							k8sObject(ctx, obj)
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans,
									[]ev1a1.Example{
										*exampleNoData,
										*exampleWithDataFilled,
									},
									[]ev1a1.Example{},
								)
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })

					})
					When("There are examples in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							var obj *ev1a1.Example
							obj = exampleNoData.DeepCopy()
							k8sObject(ctx, obj)
							obj = exampleWithData.DeepCopy()
							obj.SetNamespace("default-1")
							k8sObject(ctx, obj)
							test.code = http.StatusOK
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans,
									[]ev1a1.Example{
										*exampleNoData,
										*exampleWithDataFilled,
									},
									[]ev1a1.Example{},
								)
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
				})
			})
			When("The namespace is set", func() {
				var exampleWithDataFilled *ev1a1.Example
				BeforeEach(func() {
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/namespaces/default/examples"
					exampleWithDataFilled = setMutatedData(exampleWithData.DeepCopy(), mutatedData)
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
					When("There are no examples in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{}, []ev1a1.Example{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, exampleNoData.DeepCopy())
							k8sObject(ctx, exampleWithData.DeepCopy())
							k8sObject(ctx, exampleWrongNamespace.DeepCopy())
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in the archive, but not kubernetes", func() {
						BeforeEach(func(ctx context.Context) {
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleNoData)
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWithData)
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, exampleNoData.DeepCopy())
							k8sObject(ctx, exampleWithData.DeepCopy())
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
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
					When("There are no examples in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{}, []ev1a1.Example{})
							}
						})
						It("Should return an empty list", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in kubernetes but not the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, exampleNoData.DeepCopy())
							k8sObject(ctx, exampleWithData.DeepCopy())
							k8sObject(ctx, exampleWrongNamespace.DeepCopy())
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in the archive, but not kubernetes", func() {
						BeforeEach(func(ctx context.Context) {
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleNoData)
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWithData)
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
								})
							}
						})
						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
					When("There are examples in both kubernetes and the archive", func() {
						BeforeEach(func(ctx context.Context) {
							k8sObject(ctx, exampleNoData.DeepCopy())
							k8sObject(ctx, exampleWithData.DeepCopy())
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, exampleWrongNamespace)
							test.withBody = func(body io.Reader) {
								expectObjectList(body, new(ev1a1.Example), new(ev1a1.ExampleList), getListScans, []ev1a1.Example{
									*exampleNoData,
									*exampleWithDataFilled,
								}, []ev1a1.Example{
									*exampleWrongNamespace,
								})
							}
						})

						It("Should return them with their reports", func(ctx context.Context) { runCase(ctx) })
					})
				})
			})
		})
		When("Getting a example", func() {
			BeforeEach(func() {
				test.method = http.MethodGet
			})
			When("The namespace is blank", func() {
				BeforeEach(func(ctx context.Context) {
					k8sObject(ctx, exampleNoData.DeepCopy())
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/examples/no-reports"
				})
				When("The user does not have permission to list at cluster scope", func() {
					BeforeEach(func() {
						test.code = http.StatusForbidden
					})
					It("Should reject as forbidden", func(ctx context.Context) { runCase(ctx) })
				})
				When("The user has permission to list at cluster scope", func() {
					BeforeEach(func() {
						test.policy = clusterGetPermissions
						test.code = http.StatusNotFound
					})
					It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
				})
			})
			When("The namespace is set", func() {
				var exampleWithDataFilled *ev1a1.Example
				BeforeEach(func() {
					test.path = "/prefix/apis/ksched-internal-testing.meln5674.github.com/v1alpha1/namespaces/default/examples/test"
					exampleWithDataFilled = setMutatedData(exampleWithData.DeepCopy(), mutatedData)
					exampleWithDataFilled.Name = "test"
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
					var example *ev1a1.Example
					BeforeEach(func() {
						test.policy = namespacedGetPermissions
					})
					When("The example is not in kubernetes or the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusNotFound
						})
						It("Should reject as not found", func(ctx context.Context) { runCase(ctx) })
					})
					When("There example is in kubernetes but not the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						When("The example has no reports", func() {
							BeforeEach(func(ctx context.Context) {
								ctrl.Log.WithName("tests").Info("expected", "expected", exampleNoData)
								example = exampleNoData.DeepCopy()
								example.Name = "test"
								k8sObject(ctx, example)
								expected := exampleNoData.DeepCopy()
								expected.Name = example.Name
								test.withBody = func(body io.Reader) {
									expectObject(body, expected)
								}
							})
							It("Should return it as-is", func(ctx context.Context) { runCase(ctx) })
						})
						When("The example has a syft report", func() {
							BeforeEach(func(ctx context.Context) {
								example = exampleWithData.DeepCopy()
								example.Name = "test"
								k8sObject(ctx, example)
								test.withBody = func(body io.Reader) {
									expectObject(body, exampleWithDataFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
					})
					When("The example is in the archive but not in kubernetes", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						When("The example has no reports", func() {
							BeforeEach(func(ctx context.Context) {
								example = exampleNoData.DeepCopy()
								example.Name = "test"
								archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, example)
								test.withBody = func(body io.Reader) {
									expectObject(body, example)
								}
							})
							It("Should return it as-is", func(ctx context.Context) { runCase(ctx) })
						})
						When("The example has a syft report", func() {
							BeforeEach(func(ctx context.Context) {
								example = exampleWithData.DeepCopy()
								example.Name = "test"
								archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, example)
								test.withBody = func(body io.Reader) {
									expectObject(body, exampleWithDataFilled)
								}
							})
							It("Should return it with the report", func(ctx context.Context) { runCase(ctx) })
						})
					})
					When("The example is in both kubernetes and the archive", func() {
						BeforeEach(func() {
							test.code = http.StatusOK
						})
						BeforeEach(func(ctx context.Context) {
							example = exampleWithData.DeepCopy()
							example.Name = "test"
							example2 := exampleNoData.DeepCopy()
							example2.Name = "test"
							k8sObject(ctx, example)
							archiveObject[*ev1a1.Example, *ev1a1.ExampleList](ctx, exampleArchive, example2)
							test.withBody = func(body io.Reader) {
								expectObject(body, exampleWithDataFilled)
							}
						})
						It("Should return the example from kubernetes", func(ctx context.Context) { runCase(ctx) })
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

func (t testCase) test(api *ksched.API) {
	GinkgoHelper()
	api.TokenGetter = t.token

	policy, err := template.New("test-policy").Parse(t.policy)
	Expect(err).ToNot(HaveOccurred())
	api.RBACPolicy = &ksched.TemplatePolicy{Template: policy, Decoder: api.Decoder}

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
	// Expect((interface{}(objs[0])).(ev1a1.Example).ObjectMeta).To(Equal((interface{}(items[0])).(ev1a1.Example).ObjectMeta))
	Expect(items).To(ContainElements(objs))
	if len(badObjs) != 0 {
		for _, obj := range badObjs {
			Expect(items).ToNot(ContainElement(obj))
		}
	}
}

func getListScans(l *ev1a1.ExampleList) ([]ev1a1.Example, []*ev1a1.Example) {
	ptrs := make([]*ev1a1.Example, len(l.Items))
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
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [create]`
	namespacedCreatePermissionsTwoNamespaces = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [create]`
	namespacedGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: default
rules:
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [get, list]`
	wrongNamespacedGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: test
  namespace: not-default
rules:
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [get, list]`
	clusterGetPermissions = `
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: test
rules:
- apiGroups: [ksched-internal-testing.meln5674.github.com]
  resources: [examples]
  verbs: [get, list]`
)

func bodyFor(obj client.Object) io.Reader {
	GinkgoHelper()
	buf := bytes.NewBuffer(make([]byte, 0))
	Expect(json.NewEncoder(buf).Encode(obj)).To(Succeed())
	return buf
}

var (
	exampleMeta = metav1.TypeMeta{
		APIVersion: ev1a1.GroupVersion.Group + "/" + ev1a1.GroupVersion.Version,
		Kind:       "Example",
	}

	exampleNoData = &ev1a1.Example{
		TypeMeta: exampleMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "no-reports",
		},
		Spec: ev1a1.ExampleSpec{},
	}
	exampleWrongNamespace = &ev1a1.Example{
		TypeMeta: exampleMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "not-default",
			Name:      "no-reports",
		},
		Spec: ev1a1.ExampleSpec{},
	}
	exampleWithData = &ev1a1.Example{
		TypeMeta: exampleMeta,
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "syft-report",
		},
		Spec: ev1a1.ExampleSpec{
			HasData: true,
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

func archiveObject[O object.Object, OList client.ObjectList](ctx context.Context, archive archive.Archiver[O, OList], obj O) {
	GinkgoHelper()
	Expect(archive.ArchiveObject(ctx, obj)).To(Succeed())
	ctrl.Log.WithName("tests").Info("Added to Archive", "type", obj.GetObjectKind().GroupVersionKind(), "key", client.ObjectKeyFromObject(obj))
}

func setMutatedData(example *ev1a1.Example, data ev1a1.MutatedData) *ev1a1.Example {
	GinkgoHelper()
	example.Status.MutatedData = &data
	return example
}

//go:embed mutated-data.json
var mutatedDataBytes []byte
var mutatedData ev1a1.MutatedData
