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

// StorageVaultSpec defines the desired state of StorageVault
type StorageVaultSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"

	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_]*$`
	// +kubebuilder:validation:MaxLength=64
	// Name of the storage vault
	Name string `json:"name"`

	// Properties defines the direct key-value properties for the storage vault
	// +kubebuilder:validation:Optional
	Properties map[string]string `json:"properties,omitempty"`

	// PropertiesSecret is the name of the Kubernetes secret containing additional properties
	// +kubebuilder:validation:Optional
	PropertiesSecret string `json:"propertiesSecret,omitempty"`

	// IsDefault indicates whether this vault should be set as the default storage vault
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=false
	IsDefault *bool `json:"isDefault,omitempty"`
}

// StorageVaultStatus defines the observed state of StorageVault
type StorageVaultStatus struct {
	// Phase represents the current phase of the storage vault
	Phase string `json:"phase,omitempty"`

	// Reason provides more information about the current phase
	Reason string `json:"reason,omitempty"`

	// VaultCreated indicates whether the vault has been created in Doris
	VaultCreated bool `json:"vaultCreated,omitempty"`

	// IsDefault indicates whether this vault is set as the default storage vault
	IsDefault bool `json:"isDefault,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
//+kubebuilder:printcolumn:name="VaultCreated",type="boolean",JSONPath=".status.vaultCreated"
//+kubebuilder:printcolumn:name="IsDefault",type="boolean",JSONPath=".status.isDefault"

// StorageVault is the Schema for the storagevaults API
type StorageVault struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageVaultSpec   `json:"spec,omitempty"`
	Status StorageVaultStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StorageVaultList contains a list of StorageVault
type StorageVaultList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageVault `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StorageVault{}, &StorageVaultList{})
}
