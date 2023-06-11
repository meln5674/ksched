package supervisor

import (
	"context"
	"strings"

	example "github.com/meln5674/ksched/internal/testing/v1alpha1"
	"github.com/meln5674/ksched/pkg/object"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// InMemoryArchiver "archives" objects in memory, usually for testing purposes
type InMemoryArchiver[O object.Object, OList object.ObjectList[O]] struct {
	// Objects is a map from namespace to name to object
	Objects map[string]map[string]O
	// NewObject creates a blank object
	NewObject func() O
	// ListOf creates an object list from a slice of objects
	ListOf func([]O) OList
	// DeepCopyInto copies an object from one to anther
	DeepCopyInto func(src, dest O)
	// DeepCopy returns a copy of an object
	DeepCopy func(obj O) O
}

var _ = Archiver[*example.Example, *example.ExampleList](&InMemoryArchiver[*example.Example, *example.ExampleList]{})

// ArchiveObject implements Archiver
func (a *InMemoryArchiver[O, OList]) ArchiveObject(ctx context.Context, obj O) error {
	ns, ok := a.Objects[obj.GetNamespace()]
	if !ok {
		ns = make(map[string]O)
		a.Objects[obj.GetNamespace()] = ns
	}
	ns[obj.GetName()] = a.DeepCopy(obj)
	return nil
}

// GetObject implements Archiver
func (a *InMemoryArchiver[O, OList]) GetObject(ctx context.Context, key client.ObjectKey, obj O) error {
	ns, ok := a.Objects[key.Namespace]
	if !ok {
		return ErrNotExist
	}
	obj2, ok := ns[key.Name]
	if !ok {
		return ErrNotExist
	}
	a.DeepCopyInto(obj2, obj)
	return nil
}

// SearchObjects implements Archiver
func (a *InMemoryArchiver[O, OList]) SearchObjects(ctx context.Context, q Query, os OList) error {
	nsSet := make(map[string]struct{}, len(q.Namespaces))
	for _, ns := range q.Namespaces {
		nsSet[ns] = struct{}{}
	}
	nameSet := make(map[string]struct{}, len(q.Names))
	for _, name := range q.Names {
		nameSet[name] = struct{}{}
	}
	os.Reset(len(a.Objects))
	for ns, objects := range a.Objects {
		_, ok := nsSet[ns]
		if q.Namespaces != nil && !ok {
			continue
		}
	objects:
		for _, obj := range objects {
			_, ok := nameSet[obj.GetName()]
			if q.Names != nil && !ok {
				continue
			}
			if q.NameSubstring != "" && !strings.Contains(obj.GetName(), q.NameSubstring) {
				continue
			}
			if q.NamePrefix != "" && !strings.HasPrefix(obj.GetName(), q.NamePrefix) {
				continue
			}
			if q.NameSuffix != "" && !strings.HasSuffix(obj.GetName(), q.NameSuffix) {
				continue
			}
			objLabels := obj.GetLabels()
			for k, v := range q.Labels.MatchLabels {
				if objLabels[k] != v {
					continue objects
				}
			}
			for _, req := range q.Labels.MatchExpressions {
				req2, err := labels.NewRequirement(req.Key, selection.Operator(string(req.Operator)), req.Values)
				if err != nil {
					return err
				}
				if !req2.Matches(labels.Set(objLabels)) {
					continue objects
				}
			}

			a.DeepCopyInto(obj, os.AppendEmpty())
		}
	}

	return nil
}
