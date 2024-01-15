package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/golang-jwt/jwt/v4"
	"github.com/meln5674/gotoken"
	"github.com/meln5674/minimux"

	"github.com/golang/protobuf/proto"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/meln5674/ksched/pkg/archive"
	"github.com/meln5674/ksched/pkg/object"
)

const (
	// From https://github.com/kubernetes/kubernetes/blob/v1.28.2/staging/src/k8s.io/client-go/discovery/discovery_client.go
	openAPIV2mimePb = "application/com.github.proto-openapi.spec.v2@v1.0+protobuf"
)

// an ObjectInfo contains information and functionality related to creating and modifying Kubernetes API objects
type ObjectInfo[O client.Object, OList object.ObjectList[O]] interface {
	// New returns a new object with the TypeMeta set, and all other values zero
	New() O
	// NewList returns a new object the TypeMeta set, and no objects
	NewList() OList

	// MutateFromRead mutates an object that was read from the Kubernetes API
	MutateFromRead(ctx context.Context, obj O) error
	// MutateFromList mutates a list of objects listed from the Kubernetes API
	MutateFromList(ctx context.Context, objs OList) error
	// MutateForWrite mutates an object to be created or updated in the Kubernetes API
	MutateForWrite(ctx context.Context, obj O) error

	// Namespaced returns true if this object is namespaced, false if it is cluster scoped
	Namespaced() bool
	// GVK returns the apiVersion and kind for this kind
	GVK() schema.GroupVersionKind
	// ListGVK returns the apiVersion and kind for this kind's list kind
	ListGVK() schema.GroupVersionKind

	// Archive contains archived objects of this type. A return of nil means this kind does not have an archive.
	Archive() archive.Archiver[O, OList]
}

type NoopObjectInfo[O client.Object, OList client.ObjectList] struct {
	namespaced bool
	blank      O
	blankList  *noopObjectList[O, OList]
	gvk        schema.GroupVersionKind
	listGVK    schema.GroupVersionKind
}

type wrappedList interface {
	unwrap() client.ObjectList
	wrap(client.ObjectList)
}

var wrappedListType = reflect.TypeOf((*wrappedList)(nil)).Elem()

// noopObjectList wraps a controller-runtime ObjectList with dummy methods
// that panic so that it implements ksched's ObjectList interface.
// This allows trivial wrapping of existing k8s types to be used from ksched's
// API proxy without having to implement these methods.
// This works because the extra methods that ksched requires are only used
// if a type is mutated after being fetched, or if it can be fetched from an
// archive. If an object is only ever fetched directly from k8s, verbatim,
// these methods are never called.
type noopObjectList[O client.Object, OList client.ObjectList] struct {
	client.ObjectList
}

var _ = wrappedList((*noopObjectList[client.Object, client.ObjectList])(nil))

func (n *noopObjectList[O, OList]) Reset(cap int) {
	panic("BUG: This should never be called")
}
func (n *noopObjectList[O, OList]) Append(O) {
	panic("BUG: This should never be called")
}

func (n *noopObjectList[O, OList]) AppendEmpty() O {
	panic("BUG: This should never be called")
}

func (n *noopObjectList[O, OList]) For(func(int, O)) {
	panic("BUG: This should never be called")
}

func (n *noopObjectList[O, OList]) unwrap() client.ObjectList {
	return n.ObjectList
}
func (n *noopObjectList[O, OList]) wrap(inner client.ObjectList) {
	n.ObjectList = inner
}
func (n *noopObjectList[O, OList]) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.ObjectList)
}
func (n *noopObjectList[O, OList]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &n.ObjectList)
}

// NoopObjectInfoFor wraps a typical Object and ObjectList, e.g. *corev1.Pod and *corev1.PodList, with info that performs no mutations and has no archive
func NoopObjectInfoFor[O client.Object, OList client.ObjectList](scheme *runtime.Scheme, blank O, blankList OList, namespaced bool) (ObjectInfo[O, *noopObjectList[O, OList]], O, *noopObjectList[O, OList], error) {
	gvks, _, err := scheme.ObjectKinds(blank)
	if err != nil {
		return nil, blank, nil, err
	}
	if len(gvks) != 1 {
		return nil, blank, nil, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	listGVKs, _, err := scheme.ObjectKinds(blankList)
	if err != nil {
		return nil, blank, nil, err
	}
	if len(listGVKs) != 1 {
		return nil, blank, nil, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	noopBlankList := &noopObjectList[O, OList]{ObjectList: blankList}
	return &NoopObjectInfo[O, OList]{
		namespaced: namespaced,
		blank:      blank,
		blankList:  noopBlankList,
		gvk:        gvks[0],
		listGVK:    listGVKs[0],
	}, blank, noopBlankList, nil
}

// New returns a new object with the TypeMeta set, and all other values zero
func (n *NoopObjectInfo[O, OList]) New() O {
	return n.blank.DeepCopyObject().(O)
}

// NewList returns a new object the TypeMeta set, and no objects
func (n *NoopObjectInfo[O, OList]) NewList() *noopObjectList[O, OList] {
	return &noopObjectList[O, OList]{ObjectList: n.blankList.DeepCopyObject().(OList)}
}

// MutateFromRead mutates an object that was read from the Kubernetes API
func (n *NoopObjectInfo[O, OList]) MutateFromRead(ctx context.Context, obj O) error {
	return nil
}

// MutateFromList mutates a list of objects listed from the Kubernetes API
func (n *NoopObjectInfo[O, OList]) MutateFromList(ctx context.Context, objs *noopObjectList[O, OList]) error {
	return nil
}

// MutateForWrite mutates an object to be created or updated in the Kubernetes API
func (n *NoopObjectInfo[O, OList]) MutateForWrite(ctx context.Context, obj O) error {
	return nil
}

// Namespaced returns true if this object is namespaced, false if it is cluster scoped
func (n *NoopObjectInfo[O, OList]) Namespaced() bool {
	return n.namespaced
}

// GVK returns the apiVersion and kind for this kind
func (n *NoopObjectInfo[O, OList]) GVK() schema.GroupVersionKind {
	return n.gvk
}

// ListGVK returns the apiVersion and kind for this kind's list kind
func (n *NoopObjectInfo[O, OList]) ListGVK() schema.GroupVersionKind {
	return n.listGVK
}

// Archive contains archived objects of this type. A return of nil means this kind does not have an archive.
func (n *NoopObjectInfo[O, OList]) Archive() archive.Archiver[O, *noopObjectList[O, OList]] {
	return nil
}

// ArchivedObjectInfo is a convenience wrapper for objects which are archived, but not mutated
type ArchivedObjectInfo[O object.Object, OList object.ObjectList[O]] struct {
	namespaced bool
	blank      O
	blankList  OList
	gvk        schema.GroupVersionKind
	listGVK    schema.GroupVersionKind
	archive    archive.Archiver[O, OList]
}

// New returns a new object with the TypeMeta set, and all other values zero
func (a *ArchivedObjectInfo[O, OList]) New() O {
	return a.blank.DeepCopyObject().(O)
}

// NewList returns a new object the TypeMeta set, and no objects
func (a *ArchivedObjectInfo[O, OList]) NewList() OList {
	return a.blankList.DeepCopyObject().(OList)
}

// MutateFromRead mutates an object that was read from the Kubernetes API
func (a *ArchivedObjectInfo[O, OList]) MutateFromRead(ctx context.Context, obj O) error {
	return nil
}

// MutateFromList mutates a list of objects listed from the Kubernetes API
func (a *ArchivedObjectInfo[O, OList]) MutateFromList(ctx context.Context, objs OList) error {
	return nil
}

// MutateForWrite mutates an object to be created or updated in the Kubernetes API
func (a *ArchivedObjectInfo[O, OList]) MutateForWrite(ctx context.Context, obj O) error {
	return nil
}

// Namespaced returns true if this object is namespaced, false if it is cluster scoped
func (a *ArchivedObjectInfo[O, OList]) Namespaced() bool {
	return a.namespaced
}

// GVK returns the apiVersion and kind for this kind
func (a *ArchivedObjectInfo[O, OList]) GVK() schema.GroupVersionKind {
	return a.gvk
}

// ListGVK returns the apiVersion and kind for this kind's list kind
func (a *ArchivedObjectInfo[O, OList]) ListGVK() schema.GroupVersionKind {
	return a.listGVK
}

// Archive contains archived objects of this type. A return of nil means this kind does not have an archive.
func (a *ArchivedObjectInfo[O, OList]) Archive() archive.Archiver[O, OList] {
	return a.archive
}

// ArchivedObjectInfoFor wraps an object with info that performs no mutations but has an archive
func ArchivedObjectInfoFor[O object.Object, OList object.ObjectList[O]](scheme *runtime.Scheme, blank O, blankList OList, namespaced bool, archive archive.Archiver[O, OList]) (ObjectInfo[O, OList], O, OList, error) {
	gvks, _, err := scheme.ObjectKinds(blank)
	if err != nil {
		return nil, blank, blankList, err
	}
	if len(gvks) != 1 {
		return nil, blank, blankList, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	listGVKs, _, err := scheme.ObjectKinds(blankList)
	if err != nil {
		return nil, blank, blankList, err
	}
	if len(listGVKs) != 1 {
		return nil, blank, blankList, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	return &ArchivedObjectInfo[O, OList]{
		namespaced: namespaced,
		blank:      blank,
		blankList:  blankList,
		gvk:        gvks[0],
		listGVK:    listGVKs[0],
		archive:    archive,
	}, blank, blankList, nil
}

// Config is the static configuration for the API
type Config struct {
	// Prefix is the URL Path prefix to serve the API at
	Prefix string
}

// LoggableRequest contains the loggable parts of an http.Request, and is used to log it for debugging
type LoggableRequest struct {
	URL    url.URL
	Method string
	Header http.Header
	Token  *jwt.Token
}

type TypedAPI[O client.Object, OList object.ObjectList[O]] struct {
	*API
	info ObjectInfo[O, OList]
}

func (a *TypedAPI[O, OList]) Create(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	if (req.Namespace != "") != a.info.Namespaced() {
		a.Log.Error(nil, "Got request with mismatched scope", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	buf := bytes.NewBuffer(make([]byte, 0))
	_, err = io.Copy(buf, req.Body)
	if err != nil {
		a.Log.Error(err, "Error reading request body", "req", lr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	obj := a.info.New()
	_, _, err = a.Decoder.Decode(buf.Bytes(), nil, obj)
	if err != nil {
		a.Log.Error(err, "Error parsing request body", "req", lr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if obj.GetNamespace() != "" && obj.GetNamespace() != req.Namespace {
		a.Log.Info("Path namespace and body namespace did not match", "req", lr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	obj.SetNamespace(req.Namespace)
	err = a.info.MutateForWrite(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Error mutating object before creating in k8s", "req", lr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = a.K8s.Create(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Error creating object in k8s", "req", lr)
		handleK8sError(w, err)
		return
	}
	// controller-runtime's Create method unsets the GVK
	// because its made by people who definitely know what they're doing
	obj.GetObjectKind().SetGroupVersionKind(a.info.GVK())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(obj)
	if err != nil {
		a.Log.Error(err, "Error writing reponse body", "req", lr)
		return
	}
}

func (a *TypedAPI[O, OList]) Get(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	if (req.Namespace != "") != a.info.Namespaced() {
		a.Log.Error(nil, "Got request with mismatched scope", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	gvk := a.info.GVK()

	key := client.ObjectKey{Namespace: req.Namespace, Name: req.Name}
	obj := a.info.New()
	err = a.K8s.Get(ctx, key, obj)
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			a.Log.Error(err, "Error retreiving object from k8s", "req", lr)
			handleK8sError(w, err)
			return
		}
		a.Log.Info("Object was not in k8s", "req", lr, "error", err)
		archiver := a.info.Archive()
		if archiver == nil {
			handleK8sError(w, err)
			return
		} else {
			err = archiver.GetObject(ctx, key, obj)
			if err != nil {
				if errors.Is(err, archive.ErrNotExist) {
					a.Log.Info("Object was not in archive", "req", lr, "error", err)
					handleK8sError(w, kerrors.NewNotFound(
						schema.GroupResource{
							Group:    gvk.Group,
							Resource: gvk.Kind,
						},
						req.Name,
					))
					return
				}
				a.Log.Error(err, "Error retreiving object from archive", "req", lr)
				handleK8sError(w, err)
				return
			}
		}
	}
	err = a.info.MutateFromRead(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Failed to mutate retrieved object", "req", lr, "obj", obj)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(obj)
	if err != nil {
		a.Log.Error(err, "Error writing response body", "req", lr)
		return
	}
}

func (a *TypedAPI[O, OList]) List(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	objs := a.info.NewList()
	// Because k8s uses reflection instead of its own interface methods to look up type info, noopObjectList doesn't actually work
	// To fix this, we have to unwrap it and obtain the underlying actual object list type
	// Unfortunately, because go's type system isn't finished, we can't just use a type assertion, so we have to resort to reflection
	listDest := client.ObjectList(objs)
	objsV := reflect.ValueOf(objs)
	isWrapped := objsV.Type().Implements(wrappedListType)
	if isWrapped {
		listDest = objsV.Interface().(wrappedList).unwrap()
	}
	err = a.K8s.List(ctx, listDest, client.InNamespace(req.Namespace)) // TODO list options, TODO limit, continue
	if err != nil && !kerrors.IsNotFound(err) {
		a.Log.Error(err, "Error listing objects from k8s", "req", lr)
		handleK8sError(w, err)
		return
	}
	if isWrapped {
		objsV.Interface().(wrappedList).wrap(listDest)
	}
	archiver := a.info.Archive()
	if archiver != nil {
		objsFromArchive := a.info.NewList()
		q := archive.Query{} // TODO list options, TODO limit, continue
		if req.Namespace != "" {
			q.Namespaces = []string{req.Namespace}
		}
		err = archiver.SearchObjects(ctx, q, objsFromArchive)
		if err != nil {
			a.Log.Error(err, "Error listing objects in archive", "req", lr)
			handleK8sError(w, err)
			return
		}
		objsFromArchive.For(func(ix int, obj O) {
			objs.Append(obj)
		})
	}
	err = a.info.MutateFromList(ctx, objs)
	if err != nil {
		a.Log.Error(err, "Failed to mutate listed objects", "req", lr, "objs", objs)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	objs.GetObjectKind().SetGroupVersionKind(a.info.ListGVK())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(objs)
	if err != nil {
		a.Log.Error(err, "Error writing response body", "req", lr)
		return
	}
}

func (a *TypedAPI[O, OList]) Update(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	if (req.Namespace != "") != a.info.Namespaced() {
		a.Log.Error(nil, "Got request with mismatched scope", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	buf := bytes.NewBuffer(make([]byte, 0))
	_, err = io.Copy(buf, req.Body)
	if err != nil {
		a.Log.Error(err, "Error reading request body", "req", lr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	obj := a.info.New()
	_, _, err = a.Decoder.Decode(buf.Bytes(), nil, obj)
	if err != nil {
		a.Log.Error(err, "Error parsing request body", "req", lr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if obj.GetNamespace() != "" && obj.GetNamespace() != req.Namespace {
		a.Log.Info("Path namespace and body namespace did not match", "req", lr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	obj.SetNamespace(req.Namespace)
	err = a.info.MutateForWrite(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Failed to mutate before updating", "req", lr, "obj", obj)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = a.K8s.Update(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Error updating object in k8s", "req", lr)
		handleK8sError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(obj)
	if err != nil {
		a.Log.Error(err, "Error writing response body", "req", lr)
		return
	}
}

func (a *TypedAPI[O, OList]) Delete(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	if (req.Namespace != "") != a.info.Namespaced() {
		a.Log.Error(nil, "Got request with mismatched scope", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	obj := a.info.New()
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)
	err = a.K8s.Delete(ctx, obj)
	if err != nil {
		a.Log.Error(err, "Error removing object from k8s", "req", lr)
		handleK8sError(w, err)
		return
	}
	// controller-runtime's Create method unsets the GVK
	// because its made by people who definitely know what they're doing
	obj.GetObjectKind().SetGroupVersionKind(a.info.GVK())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(obj)
	if err != nil {
		a.Log.Error(err, "Error writing response body", "req", lr)
		return
	}
}

func (a *TypedAPI[O, OList]) DeleteCollection(ctx context.Context, req *RequestWithBody, lr LoggableRequest, w http.ResponseWriter) {
	var err error

	obj := a.info.New()
	err = a.K8s.DeleteAllOf(ctx, obj, client.InNamespace(req.Namespace))
	if err != nil {
		a.Log.Error(err, "Error removing objects from k8s", "req", lr)
		handleK8sError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type verbs interface {
	Create(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
	Get(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
	List(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
	Update(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
	Delete(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
	DeleteCollection(context.Context, *RequestWithBody, LoggableRequest, http.ResponseWriter)
}

// The API proxies Kubernetes and archives to provide
// unified access to both using RBAC from an RBAC policy
type API struct {
	// Config is the static configuration
	Config

	// Log is the logger to use
	Log logr.Logger

	// K8s is the client to access kubernetes with
	K8s client.Client
	// K8sDiscovery is the client to use for kubernetes API discovery. It should use the same connection configuration as K8s.
	K8sDiscovery *discovery.DiscoveryClient
	// RBACPolicy maps a JWT to a set of roles/cluster roles that token should act as though it has
	RBACPolicy TokenPolicy
	// Decoder decodes kubernetes YAMLs and JSON
	Decoder runtime.Decoder
	// Scheme contains type information for all relevant k8s objects
	Scheme *runtime.Scheme
	// TokenGetter obtains the JWT from a request
	TokenGetter gotoken.TokenGetter

	verbs map[schema.GroupVersionKind]verbs
}

func RegisterType[O client.Object, OList object.ObjectList[O]](a *API, urlKind string, obj O, objs OList, info ObjectInfo[O, OList]) error {
	if a.verbs == nil {
		a.verbs = make(map[schema.GroupVersionKind]verbs)
	}
	gvks, _, err := a.Scheme.ObjectKinds(info.New())
	if err != nil {
		return err
	}
	if len(gvks) != 1 {
		return fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	gvk := schema.GroupVersionKind{
		Group:   gvks[0].Group,
		Version: gvks[0].Version,
		Kind:    urlKind,
	}
	a.verbs[gvk] = &TypedAPI[O, OList]{
		info: info,
		API:  a,
	}
	a.Log.Info("Registered API type", "gvk", gvk)
	return nil
}

var (
// - {prefix}/openapi/{version}: OpenAPI discovery.
// - {prefix}/api/{version}: Legacy (core/v1) api discovery.
// - {prefix}/api/{version}/[namespaces/{namespace}/]{kind}[/{name}]: Proxy access to legacy (core/v1) k8s resources.
// - {prefix}/apis/{group}/{version}: API discovery.
// - {prefix}/apis/{group}/{version}/[namespaces/{namespace}/]{kind}[/{name}]: Proxy access to k8s resources.

)

func (a *API) Mux() *minimux.Mux {
	prefix := strings.TrimSuffix(a.Prefix, "/")

	var (

		// - {prefix}/openapi/{version}: OpenAPI discovery.
		// - {prefix}/api/[{version}/]: Legacy (core/v1) api discovery.
		// - {prefix}/api/{version}/[namespaces/{namespace}/]{kind}[/{name}]: Proxy access to legacy (core/v1) k8s resources.
		// - {prefix}/apis/[{group}[/{version}]]: API discovery.
		// - {prefix}/apis/{group}/{version}/[namespaces/{namespace}/]{kind}[/{name}]: Proxy access to k8s resources.

		openAPIDiscoveryPath = minimux.LiteralPath(prefix + "/openapi/v2")

		legacyAPIDiscoveryPath         = minimux.PathPattern(prefix + "/api/?")
		legacyAPIVersionDiscoveryPath  = minimux.PathWithVars(prefix+"/api/([^/]+)", "version")
		legacyNamespacedCollectionPath = minimux.PathWithVars(prefix+"/api/([^/]+)/namespaces/([^/]+)/([^/]+)/?", "version", "namespace", "kind")
		legacyNamespacedResourcePath   = minimux.PathWithVars(prefix+"/api/([^/]+)/namespaces/([^/]+)/([^/]+)/([^/]+)", "version", "namespace", "kind", "name")
		legacyClusterCollectionPath    = minimux.PathWithVars(prefix+"/api/([^/]+)/([^/]+)/?", "version", "kind")
		legacyClusterResourcePath      = minimux.PathWithVars(prefix+"/api/([^/]+)/([^/]+)/([^/]+)", "version", "kind", "name")

		apiDiscoveryPath             = minimux.PathPattern(prefix + "/apis/?")
		apiGroupDiscoveryPath        = minimux.PathWithVars(prefix+"/apis/([^/]+)/?", "group")
		apiGroupVersionDiscoveryPath = minimux.PathWithVars(prefix+"/apis/([^/]+)/([^/]+)/?", "group", "version")
		namespacedCollectionPath     = minimux.PathWithVars(prefix+"/apis/([^/]+)/([^/]+)/namespaces/([^/]+)/([^/]+)/?", "group", "version", "namespace", "kind")
		namespacedResourcePath       = minimux.PathWithVars(prefix+"/apis/([^/]+)/([^/]+)/namespaces/([^/]+)/([^/]+)/([^/]+)", "group", "version", "namespace", "kind", "name")
		clusterCollectionPath        = minimux.PathWithVars(prefix+"/apis/([^/]+)/([^/]+)/([^/]+)/?", "group", "version", "kind")
		clusterResourcePath          = minimux.PathWithVars(prefix+"/apis/([^/]+)/([^/]+)/([^/]+)/([^/]+)", "group", "version", "kind", "name")
	)

	openAPIRoutes := []minimux.Route{
		openAPIDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.OpenAPIDiscovery),
	}

	legacyAPIDiscoveryRoutes := []minimux.Route{
		legacyAPIDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.LegacyAPIDiscovery),
		legacyAPIVersionDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.LegacyAPIDiscovery),
	}

	///////

	legacyNamespacedCollectionRoutes := []minimux.Route{
		legacyNamespacedCollectionPath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.ListCollection),
		legacyNamespacedCollectionPath.
			WithMethods(http.MethodPost).
			IsHandledByFunc(a.CreateResource),
		legacyNamespacedCollectionPath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteCollection),
	}

	legacyNamespacedResourceRoutes := []minimux.Route{
		legacyNamespacedResourcePath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.GetResource),
		legacyNamespacedResourcePath.
			WithMethods(http.MethodPut).
			IsHandledByFunc(a.UpdateResource),
		legacyNamespacedResourcePath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteResource),
	}

	legacyClusterCollectionRoutes := []minimux.Route{
		legacyClusterCollectionPath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.ListCollection),
		legacyClusterCollectionPath.
			WithMethods(http.MethodPost).
			IsHandledByFunc(a.CreateResource),
		legacyClusterCollectionPath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteCollection),
	}

	legacyClusterResourceRoutes := []minimux.Route{
		legacyClusterResourcePath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.GetResource),
		legacyClusterResourcePath.
			WithMethods(http.MethodPut).
			IsHandledByFunc(a.UpdateResource),
		legacyClusterResourcePath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteResource),
	}

	///////

	apiDiscoveryRoutes := []minimux.Route{
		apiDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.APIDiscovery),
		apiGroupDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.APIDiscovery),
		apiGroupVersionDiscoveryPath.
			WithMethods(http.MethodGet).
			IsHandledByFunc(a.APIDiscovery),
	}

	namespacedCollectionRoutes := []minimux.Route{
		namespacedCollectionPath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.ListCollection),
		namespacedCollectionPath.
			WithMethods(http.MethodPost).
			IsHandledByFunc(a.CreateResource),
		namespacedCollectionPath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteCollection),
	}

	namespacedResourceRoutes := []minimux.Route{
		namespacedResourcePath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.GetResource),
		namespacedResourcePath.
			WithMethods(http.MethodPut).
			IsHandledByFunc(a.UpdateResource),
		namespacedResourcePath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteResource),
	}

	clusterCollectionRoutes := []minimux.Route{
		clusterCollectionPath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.ListCollection),
		clusterCollectionPath.
			WithMethods(http.MethodPost).
			IsHandledByFunc(a.CreateResource),
		clusterCollectionPath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteCollection),
	}

	clusterResourceRoutes := []minimux.Route{
		clusterResourcePath.
			WithMethods(http.MethodGet, http.MethodHead).
			IsHandledByFunc(a.GetResource),
		clusterResourcePath.
			WithMethods(http.MethodPut).
			IsHandledByFunc(a.UpdateResource),
		clusterResourcePath.
			WithMethods(http.MethodDelete).
			IsHandledByFunc(a.DeleteResource),
	}

	routes := []minimux.Route{}
	routes = append(routes, openAPIRoutes...)
	routes = append(routes, legacyAPIDiscoveryRoutes...)
	routes = append(routes, legacyNamespacedCollectionRoutes...)
	routes = append(routes, legacyNamespacedResourceRoutes...)
	routes = append(routes, legacyClusterCollectionRoutes...)
	routes = append(routes, legacyClusterResourceRoutes...)
	routes = append(routes, apiDiscoveryRoutes...)
	routes = append(routes, namespacedCollectionRoutes...)
	routes = append(routes, namespacedResourceRoutes...)
	routes = append(routes, clusterCollectionRoutes...)
	routes = append(routes, clusterResourceRoutes...)

	return &minimux.Mux{
		PreProcess:     minimux.PreProcessorChain(minimux.CancelWhenDone, a.LogPendingRequest),
		PostProcess:    a.LogCompletedRequest,
		DefaultHandler: minimux.NotFound,
		Routes:         routes,
	}
}

func (a *API) LogPendingRequest(ctx context.Context, req *http.Request) (context.Context, func()) {
	// Extra space deliberate to line up logs w/ post procesor
	a.Log.Info("Incoming  request", "method", req.Method, "url", req.URL, "user-agent", req.UserAgent())
	return ctx, func() {}
}

func (a *API) LogCompletedRequest(ctx context.Context, req *http.Request, statusCode int, err error) {
	if err != nil {
		a.Log.Error(err, "Completed request", "method", req.Method, "url", req.URL, "user-agent", req.UserAgent(), "status-code", statusCode)
	} else {
		a.Log.Info("Completed request", "method", req.Method, "url", req.URL, "user-agent", req.UserAgent(), "status-code", statusCode)
	}
}

func (a *API) OpenAPIDiscovery(ctx context.Context, w http.ResponseWriter, r *http.Request, _ map[string]string, _ error) error {
	openapiv2, err := a.K8sDiscovery.OpenAPISchema()
	if err != nil {
		a.Log.Error(err, "Failed to fetch openapi v2 schema")
		handleK8sError(w, err)
		return nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Varied-Accept", openAPIV2mimePb)
	w.Header().Set("Vary", "Accept")
	bytes, err := proto.Marshal(openapiv2)
	if err != nil {
		a.Log.Error(err, "Failed to re-marshal openapi protobuf")
		w.WriteHeader(http.StatusInternalServerError)
		return nil
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (a *API) LegacyAPIDiscovery(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	lr := LoggableRequest{
		URL:    *r.URL,
		Method: r.Method,
		Header: r.Header,
	}
	req := RequestWithBody{
		Request: Request{
			Version: pathVars["version"],
		},
	}
	if req.Version == "" {
		a.Log.Info("Performing legacy API discovery", "req", lr, "parsed", req.Request)
		// This section largely based on https://github.com/kubernetes/client-go/blob/64a35f6a46ec8a791d437495fd91c87bcd01a5b5/discovery/discovery_client.go#L233
		var responseContentType string
		body, err := a.K8sDiscovery.RESTClient().Get().
			AbsPath("/api").
			SetHeader("Accept", discovery.AcceptV1).
			Do(ctx).
			ContentType(&responseContentType).
			Raw()
		// Tolerate 404, since aggregated api servers can return it.
		if err != nil && !kerrors.IsNotFound(err) {
			a.Log.Error(err, "Legacy API discovery failed", "req", lr, "parsed", req.Request)
			handleK8sError(w, err)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(body)
		if err != nil {
			a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
		}
		return nil
	}
	a.Log.Info("Performing Legacy API Version discovery", "req", lr, "parsed", req.Request)
	resourceList, err := a.K8sDiscovery.ServerResourcesForGroupVersion(req.Version)
	if err != nil {
		a.Log.Error(err, "Legacy API Version discovery failed", "req", lr, "parsed", req.Request)
		handleK8sError(w, err)
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(resourceList)
	if err != nil {
		a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
	}
	return nil
}

func (a *API) APIDiscovery(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	lr := LoggableRequest{
		URL:    *r.URL,
		Method: r.Method,
		Header: r.Header,
	}
	req := RequestWithBody{
		Request: Request{
			Version: pathVars["version"],
			Group:   pathVars["group"],
		},
	}
	var body interface{}
	if req.Group == "" && req.Version == "" {
		a.Log.Info("Performing API Group/Version discovery", "req", lr, "parsed", req.Request)
		groupList, err := a.K8sDiscovery.ServerGroups()
		if err != nil {
			a.Log.Error(err, "API Group/Version discovery failed", "req", lr, "parsed", req.Request)
			handleK8sError(w, err)
			return nil
		}
		// The discovery client returns the legacy group (core/v1) as well as the new groups,
		// but this endpoint is only meant to return the new groups.
		// If this isn't removed, kubectl encounters a a duplicate kind error.
		if groupList.Groups[0].Name == "" {
			groupList.Groups = groupList.Groups[1:]
		}
		body = groupList
	} else {
		a.Log.Info("Performing API discovery", "req", lr, "parsed", req.Request)
		resourceList, err := a.K8sDiscovery.ServerResourcesForGroupVersion(req.Group + "/" + req.Version)
		if err != nil {
			a.Log.Error(err, "API discovery failed", "req", lr, "parsed", req.Request)
			handleK8sError(w, err)
			return nil
		}
		body = resourceList
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(body)
}

func (a *API) CreateResource(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, "create", pathVars)
	if !ok {
		return nil
	}

	verbs.Create(ctx, req, lr, w)
	return nil
}

func (a *API) ListCollection(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	verb := "list"
	if r.URL.Query().Get("watch") == "true" {
		verb = "watch"
	}
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, verb, pathVars)
	if !ok {
		return nil
	}
	if r.Method == http.MethodHead {
		return nil
	}

	verbs.List(ctx, req, lr, w)
	return nil
}

func (a *API) GetResource(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, "get", pathVars)
	if !ok {
		return nil
	}
	if r.Method == http.MethodHead {
		return nil
	}

	verbs.Get(ctx, req, lr, w)
	return nil
}

func (a *API) UpdateResource(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	// TODO: How does patch work?
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, "update", pathVars)
	if !ok {
		return nil
	}

	verbs.Update(ctx, req, lr, w)
	return nil
}

func (a *API) DeleteCollection(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, "delete-collection", pathVars)
	if !ok {
		return nil
	}

	verbs.DeleteCollection(ctx, req, lr, w)
	return nil
}

func (a *API) DeleteResource(ctx context.Context, w http.ResponseWriter, r *http.Request, pathVars map[string]string, _ error) error {
	verbs, req, lr, ok := a.ValidateRequest(ctx, w, r, "delete", pathVars)
	if !ok {
		return nil
	}

	verbs.Delete(ctx, req, lr, w)
	return nil
}

// ValidateRequest confirms that
// 1. A valid token was provided.
// 2. The policy permits the holder of that token to perform the requested verb against the resoruce or collection specified by the path variables
// 3. This API is configured to proxy the requested group/version/kind.
// If all of three conditions are met, it returns the proxied API and the request to make against it, and return true.
// If not, it will send the appropriate error back to the client, and return false.
func (a *API) ValidateRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, verb string, pathVars map[string]string) (verbs, *RequestWithBody, LoggableRequest, bool) {
	lr := LoggableRequest{
		URL:    *r.URL,
		Method: r.Method,
		Header: r.Header,
	}
	req := RequestWithBody{
		Request: Request{
			Version:   pathVars["version"],
			Group:     pathVars["group"],
			Kind:      pathVars["kind"],
			Namespace: pathVars["namespace"],
			Name:      pathVars["name"],
			Verb:      verb,
		},
		Body: r.Body,
	}
	ok, err := a.Authenticate(r, &req)
	if err != nil {
		a.Log.Error(err, "Error during authentication", "req", lr)
	}
	if !ok || err != nil {
		a.Log.Info("Request was not authenticated", "req", lr)
		w.WriteHeader(http.StatusUnauthorized)
		return nil, nil, lr, false
	}
	gvk := schema.GroupVersionKind{Group: req.Group, Version: req.Version, Kind: req.Kind}
	roles, err := a.RBACPolicy.GetRoles(req.Token)
	if err != nil {
		a.Log.Error(err, "Error preparing authorization", "req", lr, "parsed", req.Request)
		w.WriteHeader(http.StatusForbidden)
		return nil, nil, lr, false
	}
	ok = roles.Authorize(gvk, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, req.Verb)
	if !ok {
		a.Log.Info("Request is not authorized", "req", lr, "parsed", req.Request)
		w.WriteHeader(http.StatusForbidden)
		return nil, nil, lr, false
	}
	verbs, ok := a.verbs[gvk]
	if !ok {
		a.Log.Info("Got request for unknown object type", "req", lr, "parsed", req.Request)
		w.WriteHeader(http.StatusNotFound)
		return nil, nil, lr, false
	}
	return verbs, &req, lr, true
}

func (a *API) Authenticate(r *http.Request, req *RequestWithBody) (bool, error) {
	var err error
	var ok bool
	req.Token, ok, err = a.TokenGetter(r)
	return ok, err
}

func (a *API) ExecuteOnK8s(ctx context.Context, req *RequestWithBody, obj client.Object, list client.ObjectList) error {
	if req.DryRun {
		return nil
	}
	switch req.Verb {
	case "create":
		return a.K8s.Create(ctx, obj)
	case "get":
		return a.K8s.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, obj)
	case "list":
		return a.K8s.List(ctx, list) // TODO: LabelSelector
	case "update":
		return a.K8s.Update(ctx, obj)
	case "delete":
		return a.K8s.Delete(ctx, obj)
	case "deletecollection":
		return a.K8s.DeleteAllOf(ctx, obj) // TODO: LabelSelector
	}
	panic("BUG: Invalid verb")
}

type RequestType string

const (
	RequestTypeResource        = "resource"
	RequestTypeDiscovery       = "discovery"
	RequestTypeLegacyResource  = "legacy-resource"
	RequestTypeLegacyDiscovery = "legacy-discovery"
	RequestTypeOpenAPI         = "openapi"
)

const (
	LegacyPrefix  = "api"
	APIsPrefix    = "apis"
	OpenAPIPrefix = "openapi"
)

type Request struct {
	Type      RequestType
	Token     *jwt.Token
	Version   string
	Group     string
	Kind      string
	Namespace string
	Name      string
	Verb      string
	DryRun    bool
}

type RequestWithBody struct {
	Request
	Body io.Reader
}

var kerrorMap = map[int][]func(error) bool{
	http.StatusConflict:   []func(error) bool{kerrors.IsAlreadyExists, kerrors.IsConflict},
	http.StatusBadRequest: []func(error) bool{kerrors.IsBadRequest, kerrors.IsInvalid},
	http.StatusForbidden:  []func(error) bool{kerrors.IsForbidden},
	http.StatusGone:       []func(error) bool{kerrors.IsGone, kerrors.IsResourceExpired},
	http.StatusInternalServerError: []func(error) bool{
		kerrors.IsInternalError,
		kerrors.IsServerTimeout,
		kerrors.IsServiceUnavailable,
		kerrors.IsTimeout,
		kerrors.IsTooManyRequests,
		kerrors.IsUnexpectedServerError,
		kerrors.IsUnexpectedObjectError,
	},
	http.StatusMethodNotAllowed:      []func(error) bool{kerrors.IsMethodNotSupported},
	http.StatusNotAcceptable:         []func(error) bool{kerrors.IsNotAcceptable},
	http.StatusRequestEntityTooLarge: []func(error) bool{kerrors.IsRequestEntityTooLargeError},
	http.StatusTooManyRequests:       []func(error) bool{kerrors.IsTooManyRequests},
	http.StatusNotFound:              []func(error) bool{kerrors.IsNotFound},
	http.StatusUnsupportedMediaType:  []func(error) bool{kerrors.IsUnsupportedMediaType},
}

func handleK8sError(w http.ResponseWriter, err error) {
	for code, checks := range kerrorMap {
		for _, check := range checks {
			if check(err) {
				w.WriteHeader(code)
				w.Write([]byte(kerrors.ReasonForError(err)))
				return
			}
		}
	}
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("Unknown Error"))
}
