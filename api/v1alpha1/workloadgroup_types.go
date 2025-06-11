/*
Copyright 2024.

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
)

// WorkloadGroupSpec defines the desired state of WorkloadGroup
type WorkloadGroupSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"

	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9-_]*$`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Workload group name is immutable"
	// Name of the workload group
	Name string `json:"name"`

	// Properties defines the direct key-value properties for the workload group
	// +kubebuilder:validation:Optional
	Properties map[string]string `json:"properties,omitempty"`
}

// WorkloadGroupStatus defines the observed state of WorkloadGroup
type WorkloadGroupStatus struct {
	// Phase represents the current phase of the workload group
	Phase string `json:"phase,omitempty"`

	// Reason provides more information about the current phase
	Reason string `json:"reason,omitempty"`

	// WorkloadGroupCreated indicates whether the workload group has been created in Doris
	WorkloadGroupCreated bool `json:"workloadGroupCreated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Created",type="boolean",JSONPath=".status.workloadGroupCreated"

// WorkloadGroup is the Schema for the workloadgroups API
type WorkloadGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkloadGroupSpec   `json:"spec,omitempty"`
	Status WorkloadGroupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// WorkloadGroupList contains a list of WorkloadGroup
type WorkloadGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkloadGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkloadGroup{}, &WorkloadGroupList{})
}
