package archive

import (
	"context"
	"errors"
	"fmt"

	example "github.com/meln5674/ksched/internal/testing/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	ErrNotExist = errors.New("No such object")
)

// An Archiver and archive Kubernetes objects in another data store, and then retrieve them later, to reduce load on etcd.
type Archiver[O client.Object, OList client.ObjectList] interface {
	// ArchiveObject adds an object to the archive
	ArchiveObject(ctx context.Context, obj O) error
	// GetObject retrieves a single object from the archive, or returns ErrNotExist if it's not there
	GetObject(ctx context.Context, key client.ObjectKey, obj O) error
	// SearchObjects returns all archived objects which meet a set of criteria. It must return no error if no objects match.
	SearchObjects(ctx context.Context, q Query, os OList) error
}

// A Query represents criteria for objects to retrieve from an archive.
// It is meant to closely mimic what is available from the Kubernetes API server,
// with a few extra conveniences for archive UIs and APIs.
// In pseudo-SQL, the resulting query should be:
// WHERE (metadata.namespace IN Namespaces)
// AND (metadata.name in Names)
// AND (metadata.name LIKE '%NameSubstring%')
// AND (metadata.name LIKE 'NamePrefix%')
// AND (metadata.name LIKE '%NameSuffix')
// AND (metadata.labels MATCHES Labels)
// where MATCHES is a fictional function that works like the Kubernetes API server for label matching
// TODO: This should be replaced with ListOptions
type Query struct {
	Namespaces    []string
	NameSubstring string
	NamePrefix    string
	NameSuffix    string
	Names         []string
	Labels        metav1.LabelSelector
}

// NoopArchive does nothing and returns errors when attempting to use it.
type NoopArchiver[O client.Object, OList client.ObjectList] struct{}

var _ = Archiver[*example.Example, *example.ExampleList](&NoopArchiver[*example.Example, *example.ExampleList]{})

// ArchiveObject implements Archiver
func (n *NoopArchiver[O, OList]) ArchiveObject(context.Context, O) error {
	return fmt.Errorf("Archive not enabled")
}

// GetObject implements Archiver
func (n *NoopArchiver[O, OList]) GetObject(context.Context, client.ObjectKey, O) error {
	return ErrNotExist
}

// SearchObjects implements Archiver
func (n *NoopArchiver[O, OList]) SearchObjects(context.Context, Query, OList) error {
	return fmt.Errorf("Archive not enabled")
}
