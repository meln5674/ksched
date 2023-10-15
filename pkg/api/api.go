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
	"strings"

	"github.com/go-logr/logr"
	"github.com/golang-jwt/jwt/v4"
	"github.com/meln5674/gotoken"

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
	blankList  noopObjectList[O, OList]
	gvk        schema.GroupVersionKind
	listGVK    schema.GroupVersionKind
}

type noopObjectList[O client.Object, OList client.ObjectList] struct {
	client.ObjectList
}

func (n noopObjectList[O, OList]) Reset(cap int) {
	panic("BUG: This should never be called")
}
func (n noopObjectList[O, OList]) Append(O) {
	panic("BUG: This should never be called")
}

func (n noopObjectList[O, OList]) AppendEmpty() O {
	panic("BUG: This should never be called")
}

func (n noopObjectList[O, OList]) For(func(int, O)) {
	panic("BUG: This should never be called")
}

// NoopObjectInfoFor wraps a typical Object and ObjectList, e.g. *corev1.Pod and *corev1.PodList, with info that performs no mutations and has no archive
func NoopObjectInfoFor[O client.Object, OList client.ObjectList](scheme *runtime.Scheme, blank O, blankList OList, namespaced bool) (ObjectInfo[O, noopObjectList[O, OList]], O, noopObjectList[O, OList], error) {
	gvks, _, err := scheme.ObjectKinds(blank)
	if err != nil {
		return nil, blank, noopObjectList[O, OList]{}, err
	}
	if len(gvks) != 1 {
		return nil, blank, noopObjectList[O, OList]{}, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	listGVKs, _, err := scheme.ObjectKinds(blankList)
	if err != nil {
		return nil, blank, noopObjectList[O, OList]{}, err
	}
	if len(listGVKs) != 1 {
		return nil, blank, noopObjectList[O, OList]{}, fmt.Errorf("Did not get exactly one GroupVersionKind")
	}
	noopBlankList := noopObjectList[O, OList]{ObjectList: blankList}
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
func (n *NoopObjectInfo[O, OList]) NewList() noopObjectList[O, OList] {
	return noopObjectList[O, OList]{ObjectList: n.blankList.DeepCopyObject().(OList)}
}

// MutateFromRead mutates an object that was read from the Kubernetes API
func (n *NoopObjectInfo[O, OList]) MutateFromRead(ctx context.Context, obj O) error {
	return nil
}

// MutateFromList mutates a list of objects listed from the Kubernetes API
func (n *NoopObjectInfo[O, OList]) MutateFromList(ctx context.Context, objs noopObjectList[O, OList]) error {
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
func (n *NoopObjectInfo[O, OList]) Archive() archive.Archiver[O, noopObjectList[O, OList]] {
	return nil
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
	err = a.K8s.List(ctx, objs, client.InNamespace(req.Namespace)) // TODO list options, TODO limit, continue
	if err != nil && !kerrors.IsNotFound(err) {
		a.Log.Error(err, "Error listing objects from k8s", "req", lr)
		handleK8sError(w, err)
		return
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

// ServeHTTP implements http.Handler.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	lr := LoggableRequest{
		URL:    *r.URL,
		Method: r.Method,
		Header: r.Header,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !strings.HasPrefix(r.URL.Path, a.Prefix) {
		a.Log.Info("Request does not have prefix", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	r.URL.Path = strings.TrimPrefix(r.URL.Path, a.Prefix)
	defer func() { r.URL.Path = a.Prefix + r.URL.Path }()

	var req RequestWithBody
	ok, err := a.Authenticate(r, &req)
	if err != nil {
		a.Log.Error(err, "Error during authentication", "req", lr)
	}
	if !ok || err != nil {
		a.Log.Info("Request was not authenticated", "req", lr)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	ok, err = a.ParseRequest(r, lr, &req)
	if err != nil {
		a.Log.Error(err, "Error parsing request", "req", lr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !ok {
		a.Log.Info("Request method/path could not be parsed", "req", lr)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch req.Type {
	case RequestTypeResource:
		a.Log.Info("Parsed method/path/gvk", "req", lr, "parsed", req.Request)
		roles, err := a.RBACPolicy.GetRoles(req.Token)
		if err != nil {
			a.Log.Error(err, "Error preparing authorization", "req", lr, "parsed", req.Request)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		gvk := schema.GroupVersionKind{Group: req.Group, Version: req.Version, Kind: req.Kind}
		ok = roles.Authorize(gvk, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, req.Verb)
		if !ok {
			a.Log.Info("Request is not authorized", "req", lr, "parsed", req.Request)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		verbs, ok := a.verbs[gvk]
		if !ok {
			a.Log.Info("Got request for unknown object type", "req", lr, "parsed", req.Request)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch req.Verb {
		case "create":
			verbs.Create(ctx, &req, lr, w)
			return
		case "get":
			verbs.Get(ctx, &req, lr, w)
			return
		case "list":
			verbs.List(ctx, &req, lr, w)
			return
		case "update":
			verbs.Update(ctx, &req, lr, w)
			return
		case "delete":
			verbs.Delete(ctx, &req, lr, w)
			return
		case "deletecollection":
			verbs.DeleteCollection(ctx, &req, lr, w)
			return
		default:
			panic(fmt.Sprintf("BUG: Invalid verb %s", req.Verb))
		}
	// TODO: Replace this all with just a reverse proxy
	case RequestTypeLegacyDiscovery:
		if req.Version == "" {
			a.Log.Info("Performing legacy API discovery", "req", lr, "parsed", req.Request)
			// This section largely based on https://github.com/kubernetes/client-go/blob/64a35f6a46ec8a791d437495fd91c87bcd01a5b5/discovery/discovery_client.go#L233
			var responseContentType string
			body, err := a.K8sDiscovery.RESTClient().Get().
				AbsPath("/api").
				SetHeader("Accept", discovery.AcceptV1).
				Do(context.TODO()).
				ContentType(&responseContentType).
				Raw()
			// Tolerate 404, since aggregated api servers can return it.
			if err != nil && !kerrors.IsNotFound(err) {
				a.Log.Error(err, "Legacy API discovery failed", "req", lr, "parsed", req.Request)
				handleK8sError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err = w.Write(body)
			if err != nil {
				a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
				return
			}
		} else {
			a.Log.Info("Performing Legacy API Version discovery", "req", lr, "parsed", req.Request)
			resourceList, err := a.K8sDiscovery.ServerResourcesForGroupVersion(req.Version)
			if err != nil {
				a.Log.Error(err, "Legacy API Version discovery failed", "req", lr, "parsed", req.Request)
				handleK8sError(w, err)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(resourceList)
			if err != nil {
				a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
				return
			}
		}
	case RequestTypeDiscovery:
		var body interface{}
		if req.Group == "" && req.Version == "" {
			a.Log.Info("Performing API Group/Version discovery", "req", lr, "parsed", req.Request)
			groupList, err := a.K8sDiscovery.ServerGroups()
			if err != nil {
				a.Log.Error(err, "API Group/Version discovery failed", "req", lr, "parsed", req.Request)
				handleK8sError(w, err)
				return
			}
			body = groupList
		} else {
			a.Log.Info("Performing API discovery", "req", lr, "parsed", req.Request)
			resourceList, err := a.K8sDiscovery.ServerResourcesForGroupVersion(req.Group + "/" + req.Version)
			if err != nil {
				a.Log.Error(err, "API discovery failed", "req", lr, "parsed", req.Request)
				handleK8sError(w, err)
				return
			}
			body = resourceList
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(body)
		if err != nil {
			a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
			return
		}
	case RequestTypeOpenAPI:
		switch req.Version {
		case "v2":
			openapiv2, err := a.K8sDiscovery.OpenAPISchema()
			if err != nil {
				a.Log.Error(err, "Failed to fetch openapi v2 schema")
				handleK8sError(w, err)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Varied-Accept", openAPIV2mimePb)
			w.Header().Set("Vary", "Accept")
			w.WriteHeader(http.StatusOK)
			bytes, err := proto.Marshal(openapiv2)
			if err != nil {
				a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
				return
			}
			_, err = w.Write(bytes)
			if err != nil {
				a.Log.Error(err, "Error writing reponse body", "req", lr, "parsed", req.Request)
				return
			}
		default:
			a.Log.Error(nil, "Got request with unknown openapi version", "req", lr)
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}
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
	RequestTypeLegacyDiscovery = "legacy-discovery"
	RequestTypeDiscovery       = "discovery"
	RequestTypeOpenAPI         = "openapi"
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
	Body      io.Reader
}

type RequestWithBody struct {
	Request
	Body io.Reader
}

func ParseLegacyDiscoveryPath(parts []string, r *RequestWithBody) bool {
	if len(parts) == 0 {
		r.Type = RequestTypeLegacyDiscovery
		return true
	}
	if len(parts) == 1 {
		r.Type = RequestTypeLegacyDiscovery
		r.Version = parts[0]
		return true
	}
	return false
}

func ParseDiscoveryPath(parts []string, r *RequestWithBody) bool {
	if len(parts) == 0 {
		r.Type = RequestTypeDiscovery
		return true
	}
	if len(parts) == 2 {
		r.Type = RequestTypeDiscovery
		r.Group = parts[0]
		r.Version = parts[1]
		return true
	}
	return false
}

func ParseOpenAPIPath(parts []string, r *RequestWithBody) bool {
	if len(parts) == 1 {
		r.Type = RequestTypeOpenAPI
		r.Version = parts[0]
		return true
	}
	return false
}

func ParseNamespacedSingleResourcePath(parts []string, r *RequestWithBody) bool {
	if len(parts) != 6 {
		return false
	}
	if parts[2] != "namespaces" {
		return false
	}
	r.Type = RequestTypeResource
	r.Group = parts[0]
	r.Version = parts[1]
	r.Namespace = parts[3]
	r.Kind = parts[4]
	r.Name = parts[5]
	return true
}

func ParseClusterSingleResourcePath(parts []string, r *RequestWithBody) bool {
	if len(parts) != 4 {
		return false
	}
	r.Type = RequestTypeResource
	r.Group = parts[0]
	r.Version = parts[1]
	r.Kind = parts[2]
	r.Name = parts[3]
	return true
}

func ParseNamespacedAllResourcesPath(parts []string, r *RequestWithBody) bool {
	if len(parts) != 5 {
		return false
	}
	if parts[2] != "namespaces" {
		return false
	}
	r.Type = RequestTypeResource
	r.Group = parts[0]
	r.Version = parts[1]
	r.Namespace = parts[3]
	r.Kind = parts[4]
	return true
}

func ParseClusterAllResourcesPath(parts []string, r *RequestWithBody) bool {
	if len(parts) != 3 {
		return false
	}
	r.Type = RequestTypeResource
	r.Group = parts[0]
	r.Version = parts[1]
	r.Kind = parts[2]
	return true
}

const LegacyPrefix = "api"
const APIsPrefix = "apis"
const OpenAPIPrefix = "openapi"

func (a *API) ParseRequest(r *http.Request, lr LoggableRequest, req *RequestWithBody) (bool, error) {
	// URL Structure:
	// - {prefix}/api/{group}/{version}/[namespaces/{namespace}/]{kind}[/{name}]: Proxy access to k8s resources. Requires authoriziation.
	// - {prefix}/apis[/...]: Proxy direct request to k8s for api info. No authorization required
	// {namespace} is empty for cluster-scoped actions

	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/"), "/")
	a.Log.Info("Parsing URL parts", "req", lr, "parts", parts)

	if parts[0] == OpenAPIPrefix && r.Method == http.MethodGet {
		if ParseOpenAPIPath(parts[1:], req) {
			return true, nil
		}
		return false, nil
	} else if parts[0] == LegacyPrefix && r.Method == http.MethodGet {
		if ParseLegacyDiscoveryPath(parts[1:], req) {
			return true, nil
		}
		return false, nil
	} else if parts[0] == APIsPrefix {
		switch r.Method {
		case http.MethodHead:
			// Checking permissions to single resource or listing all resources by type, used by UI
			if ParseNamespacedSingleResourcePath(parts[1:], req) {
				req.Verb = "get"
			} else if ParseNamespacedAllResourcesPath(parts[1:], req) {
				req.Verb = "list"
			} else if ParseClusterSingleResourcePath(parts[1:], req) {
				req.Verb = "get"
			} else if ParseClusterAllResourcesPath(parts[1:], req) {
				req.Verb = "list"
			} else {
				a.Log.Info("Can't parse HEAD as either single or all resources path", "req", lr)
				return false, nil
			}
			req.DryRun = true
			return true, nil
		case http.MethodPost:
			// Creating a resource
			if !ParseNamespacedAllResourcesPath(parts[1:], req) {
				a.Log.Info("Can't parse POST as all resources path", "req", lr)
				return false, nil
			}
			req.Verb = "create"
			req.Body = r.Body
			return true, nil
		case http.MethodGet:
			// Either a reading single resource by name, or a list all of resource type
			if ParseDiscoveryPath(parts[1:], req) {
				// Discovery all apis or discovery group/version
				return true, nil
			} else if ParseNamespacedSingleResourcePath(parts[1:], req) {
				// Get single namespaced object
				req.Verb = "get"
			} else if ParseNamespacedAllResourcesPath(parts[1:], req) {
				// List all objects in namespace
				req.Verb = "list"
			} else if ParseClusterSingleResourcePath(parts[1:], req) {
				// Get single cluster-scoped object
				req.Verb = "get"
			} else if ParseClusterAllResourcesPath(parts[1:], req) {
				// List all objects in all namespaces
				req.Verb = "list"
			} else {
				a.Log.Info("Can't parse GET as either single or multiple object path", "req", lr)
				return false, nil
			}
			return true, nil
		case http.MethodPut:
			// Update a single resource by name
			if !ParseNamespacedSingleResourcePath(parts[1:], req) {
				a.Log.Info("Can't parse PUT as single resource path", "req", lr)
				return false, nil
			}
			req.Verb = "update"
			req.Body = r.Body
			return true, nil
		case http.MethodDelete:
			// Either delete a single resource by name, or delete all resources in namespace
			if ParseNamespacedSingleResourcePath(parts[1:], req) {
				req.Verb = "delete"
			} else if ParseNamespacedAllResourcesPath(parts[1:], req) {
				req.Verb = "deletecollection"
			} else if ParseNamespacedSingleResourcePath(parts[1:], req) {
				req.Verb = "delete"
			} else if ParseNamespacedAllResourcesPath(parts[1:], req) {
				req.Verb = "deletecollection"
			} else {
				a.Log.Info("Can't parse DELETE as either single or multiple object path", "req", lr)
				return false, nil
			}
			return true, nil
		default:
			a.Log.Info("Unknown method", "req", lr)
			return false, nil
		}
	} else {
		a.Log.Info("Unknown path", "req", lr)
		return false, nil
	}

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
