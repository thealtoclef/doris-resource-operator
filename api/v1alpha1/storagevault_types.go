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

// StorageVaultType represents the type of storage vault
type StorageVaultType string

const (
	// S3Vault represents an S3-compatible storage vault
	S3Vault StorageVaultType = "S3"
	// HDFSVault represents an HDFS storage vault (not supported yet)
	HDFSVault StorageVaultType = "HDFS"
)

// S3Provider represents the cloud provider for S3-compatible storage
type S3Provider string

const (
	// S3ProviderAWS represents Amazon S3
	S3ProviderAWS S3Provider = "S3"
	// S3ProviderOSS represents Aliyun OSS
	S3ProviderOSS S3Provider = "OSS"
	// S3ProviderCOS represents Tencent COS
	S3ProviderCOS S3Provider = "COS"
	// S3ProviderOBS represents Huawei OBS
	S3ProviderOBS S3Provider = "OBS"
	// S3ProviderBOS represents Baidu BOS
	S3ProviderBOS S3Provider = "BOS"
	// S3ProviderAzure represents Azure Blob Storage
	S3ProviderAzure S3Provider = "AZURE"
	// S3ProviderGCP represents Google Cloud Storage
	S3ProviderGCP S3Provider = "GCP"
)

// S3Properties defines the properties for S3-compatible storage vault
type S3Properties struct {
	// Endpoint is the endpoint used for object storage (without http:// or https://)
	// +kubebuilder:validation:Required
	Endpoint string `json:"endpoint"`

	// Region is the region of your bucket
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// RootPath is the path where the data would be stored
	// +kubebuilder:validation:Required
	RootPath string `json:"rootPath"`

	// Bucket is the bucket of your object storage account
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Provider is the cloud vendor providing the object storage service
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=S3;OSS;COS;OBS;BOS;AZURE;GCP
	Provider S3Provider `json:"provider"`

	// UsePathStyle indicates whether to use path-style URL
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=true
	UsePathStyle *bool `json:"usePathStyle,omitempty"`

	// AccessKeySecretRef is the secret reference for the S3 access key
	// +kubebuilder:validation:Optional
	AccessKeySecretRef *SecretRef `json:"accessKeySecretRef,omitempty"`

	// SecretKeySecretRef is the secret reference for the S3 secret key
	// +kubebuilder:validation:Optional
	SecretKeySecretRef *SecretRef `json:"secretKeySecretRef,omitempty"`
}

// StorageVaultSpec defines the desired state of StorageVault
type StorageVaultSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"

	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_]*$`
	// +kubebuilder:validation:MaxLength=64
	// Name of the storage vault
	Name string `json:"name"`

	// +kubebuilder:validation:Enum=S3
	// +kubebuilder:validation:XValidation:rule="self != 'HDFS'",message="HDFS storage vault is not supported yet"
	// Type of storage vault (only S3 is supported)
	Type StorageVaultType `json:"type"`

	// S3Properties defines the properties for S3-compatible storage vault
	// +kubebuilder:validation:Required
	S3Properties *S3Properties `json:"s3Properties,omitempty"`

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
