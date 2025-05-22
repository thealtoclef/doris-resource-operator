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

// VaultType defines the type of storage vault
// +kubebuilder:validation:Enum=S3;HDFS
type VaultType string

const (
	// S3VaultType represents an S3-compatible storage vault
	S3VaultType VaultType = "S3"
	// HDFSVaultType represents an HDFS storage vault (reserved for future implementation)
	HDFSVaultType VaultType = "HDFS"
)

// S3Provider defines the supported S3 providers
// +kubebuilder:validation:Enum=S3;OSS;COS;OBS;BOS;AZURE;GCP
type S3Provider string

const (
	// S3ProviderS3 is AWS S3
	S3ProviderS3 S3Provider = "S3"
	// S3ProviderOSS is Alibaba Cloud OSS
	S3ProviderOSS S3Provider = "OSS"
	// S3ProviderCOS is Tencent Cloud COS
	S3ProviderCOS S3Provider = "COS"
	// S3ProviderOBS is Huawei Cloud OBS
	S3ProviderOBS S3Provider = "OBS"
	// S3ProviderBOS is Baidu Cloud BOS
	S3ProviderBOS S3Provider = "BOS"
	// S3ProviderAZURE is Microsoft Azure Storage
	S3ProviderAZURE S3Provider = "AZURE"
	// S3ProviderGCP is Google Cloud Storage
	S3ProviderGCP S3Provider = "GCP"
)

// S3Properties defines the properties for an S3 storage vault
type S3Properties struct {
	// Endpoint is the S3 endpoint
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Endpoint is immutable"
	Endpoint string `json:"endpoint"`

	// Region is the S3 region
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Region is immutable"
	Region string `json:"region"`

	// RootPath is the path where the data would be stored
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="RootPath is immutable"
	RootPath string `json:"rootPath"`

	// Bucket is the S3 bucket name (StorageAccount for Azure)
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Bucket is immutable"
	Bucket string `json:"bucket"`

	// AuthSecret is the name of the Kubernetes secret containing access_key and secret_key
	// This field is mutable to allow credential rotation
	AuthSecret string `json:"authSecret"`

	// Provider is the cloud vendor which provides the object storage service
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Provider is immutable"
	Provider S3Provider `json:"provider"`

	// +kubebuilder:default=true

	// UsePathStyle indicates using path-style URL (true) or virtual-hosted-style URL (false)
	// This field is mutable to allow changing the URL style
	UsePathStyle *bool `json:"usePathStyle,omitempty"`
}

// StorageVaultSpec defines the desired state of StorageVault
// +kubebuilder:validation:XValidation:rule="self.type != 'S3' || has(self.s3Properties)",message="S3Properties must be provided when Type is S3"
type StorageVaultSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"
	// Cluster name to reference to, which decides the destination
	// +kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_]*$`
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Required
	// Name of the storage vault
	// This field is mutable to allow renaming the storage vault
	Name string `json:"name"`

	// Type of storage vault
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Type is immutable"
	Type VaultType `json:"type"`

	// S3Properties contains configuration for S3 vault type
	// +kubebuilder:validation:Optional
	S3Properties *S3Properties `json:"s3Properties,omitempty"`

	// IsDefault indicates whether this vault should be set as the default storage vault
	// Only one storage vault can be the default in a Doris cluster at any time.
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

	// StorageVaultCreated indicates whether the storage vault has been created in Doris
	StorageVaultCreated bool `json:"storageVaultCreated,omitempty"`

	// IsDefault indicates whether this vault is set as the default storage vault
	IsDefault bool `json:"isDefault,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Created",type="boolean",JSONPath=".status.storageVaultCreated"
// +kubebuilder:printcolumn:name="Default",type="boolean",JSONPath=".status.isDefault"

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
