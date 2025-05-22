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

// GlobalVariableSpec defines the desired state of GlobalVariable
type GlobalVariableSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"
	// +kubebuilder:validation:Required
	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Variable name is immutable"
	// +kubebuilder:validation:Required
	// Name of the global variable
	Name string `json:"name"`

	// Value of the global variable
	// +kubebuilder:validation:Required
	Value string `json:"value"`
}

// GlobalVariableStatus defines the observed state of GlobalVariable
type GlobalVariableStatus struct {
	// Phase represents the current phase of the GlobalVariable
	Phase string `json:"phase,omitempty"`

	// Reason represents the reason for the current phase
	Reason string `json:"reason,omitempty"`

	// VariableCreated indicates whether the variable has been created
	VariableCreated bool `json:"variableCreated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Created",type="boolean",JSONPath=".status.variableCreated"

// GlobalVariable is the Schema for the globalvariables API
type GlobalVariable struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GlobalVariableSpec   `json:"spec,omitempty"`
	Status GlobalVariableStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// GlobalVariableList contains a list of GlobalVariable
type GlobalVariableList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GlobalVariable `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GlobalVariable{}, &GlobalVariableList{})
}
