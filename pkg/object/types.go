package object

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Object interface {
	client.Object
	AssignTo(name string)
	AssignedTo() string
	Complete(now metav1.Time, successful bool)
	CompletedAt() *metav1.Time
	Successful() bool
}

type ObjectList[O client.Object] interface {
	client.ObjectList
	Reset(cap int)
	Append(O)
	AppendEmpty() O
	For(func(int, O))
}
