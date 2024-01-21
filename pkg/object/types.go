package object

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Object interface {
	client.Object
	AssignTo(name string)
	AssignedTo() string
	Complete(now time.Time, successful bool)
	CompletedAt() *time.Time
	Successful() bool
}

type ObjectList[O client.Object] interface {
	client.ObjectList
	Reset(cap int)
	Append(O)
	AppendEmpty() O
	For(func(int, O))
}

func FromOptionalTime(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	t2 := t.Time
	return &t2
}

func FromOptionalMicroTime(t *metav1.MicroTime) *time.Time {
	if t == nil {
		return nil
	}
	t2 := t.Time
	return &t2
}
