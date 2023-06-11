package executor

import (
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/meln5674/ksched/pkg/object"
)

type ExecutorNamePredicate[O object.Object] struct {
	Name string
}

func (p *ExecutorNamePredicate[O]) Matches(object client.Object) bool {
	exObj, ok := object.(O)
	if !ok {
		return true
	}
	return exObj.AssignedTo() == p.Name
}

func (p *ExecutorNamePredicate[O]) Create(e event.CreateEvent) bool {
	return p.Matches(e.Object)
}

func (p *ExecutorNamePredicate[O]) Delete(e event.DeleteEvent) bool {
	return p.Matches(e.Object)
}

func (p *ExecutorNamePredicate[O]) Update(e event.UpdateEvent) bool {
	return p.Matches(e.ObjectNew)
}

func (p *ExecutorNamePredicate[O]) Generic(e event.GenericEvent) bool {
	return p.Matches(e.Object)
}

type ExecutorConfig struct {
	Name string
}

type Executor[O object.Object] struct {
	ExecutorConfig
	NewObject func() O
}

// BuilderForManager starts setting up the controller with the Manager.
func (e *Executor[O]) BuilderForManager(mgr ctrl.Manager) *builder.Builder {
	return ctrl.NewControllerManagedBy(mgr).
		For(e.NewObject()).
		WithEventFilter(&ExecutorNamePredicate[O]{Name: e.Name})
}
