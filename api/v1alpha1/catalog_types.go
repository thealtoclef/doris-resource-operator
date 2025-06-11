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

// CatalogSpec defines the desired state of Catalog
type CatalogSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"

	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9-_]*$`
	// +kubebuilder:validation:MaxLength=64
	// Name of the catalog
	Name string `json:"name"`

	// Comment for the catalog
	// +kubebuilder:validation:Optional
	Comment string `json:"comment,omitempty"`

	// Properties defines the direct key-value properties for the catalog
	// +kubebuilder:validation:Optional
	Properties map[string]string `json:"properties,omitempty"`

	// PropertiesSecret is the name of the Kubernetes secret containing additional properties
	// +kubebuilder:validation:Optional
	PropertiesSecret string `json:"propertiesSecret,omitempty"`
}

// CatalogStatus defines the observed state of Catalog
type CatalogStatus struct {
	// Phase represents the current phase of the catalog
	Phase string `json:"phase,omitempty"`

	// Reason provides more information about the current phase
	Reason string `json:"reason,omitempty"`

	// CatalogCreated indicates whether the catalog has been created in Doris
	CatalogCreated bool `json:"catalogCreated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Created",type="boolean",JSONPath=".status.catalogCreated"

// Catalog is the Schema for the catalogs API
type Catalog struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CatalogSpec   `json:"spec,omitempty"`
	Status CatalogStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CatalogList contains a list of Catalog
type CatalogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Catalog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Catalog{}, &CatalogList{})
}
