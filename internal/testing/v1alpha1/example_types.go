/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	"github.com/meln5674/ksched/pkg/object"
)

type ExampleSpec struct {
	// +optional
	AssignedTo string `json:"assignedTo,omitempty"`
	HasData    bool   `json:"hasData,omitempty"`
}

type MutatedData struct {
	Foo string `json:"foo"`
}

type ExampleStatus struct {
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	Successful  bool         `json:"successful,omitempty"`
	MutatedData *MutatedData `json:"mutatedData,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

type Example struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ExampleSpec   `json:"spec,omitempty"`
	Status ExampleStatus `json:"status,omitempty"`
}

func (e *Example) AssignTo(name string) {
	e.Spec.AssignedTo = name
}

func (e *Example) AssignedTo() string {
	return e.Spec.AssignedTo
}

func (e *Example) Complete(now time.Time, successful bool) {
	t := metav1.NewTime(now)
	e.Status.CompletedAt = &t
	e.Status.Successful = successful
}

func (e *Example) CompletedAt() *time.Time {
	return object.FromOptionalTime(e.Status.CompletedAt)
}

func (e *Example) Successful() bool {
	return e.Status.Successful
}

//+kubebuilder:object:root=true

// ExampleList contains a list of Example
type ExampleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Example `json:"items"`
}

func (e *ExampleList) AppendEmpty() *Example {
	ix := len(e.Items)
	e.Items = append(e.Items, Example{})
	return &e.Items[ix]
}

func (e *ExampleList) Append(item *Example) {
	e.Items = append(e.Items, *item)
}

func (e *ExampleList) Reset(cap int) {
	e.Items = make([]Example, 0, cap)
}

func (e *ExampleList) For(f func(int, *Example)) {
	for ix := range e.Items {
		f(ix, &e.Items[ix])
	}
}

func init() {
	SchemeBuilder.Register(&Example{}, &ExampleList{})
}
