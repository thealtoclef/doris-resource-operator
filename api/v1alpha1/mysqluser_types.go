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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Grant defines the privileges and the resource for a MySQL user
type Grant struct {

	// Privileges to grant to the user
	Privileges []string `json:"privileges"`

	// Target on which the privileges are applied
	Target string `json:"target"`
}

// Property defines a single property for a MySQL user
type Property struct {
	// +kubebuilder:validation:Required
	// Name of the property
	Name string `json:"name"`

	// +kubebuilder:validation:Required
	// Value of the property
	Value string `json:"value"`
}

// MySQLUserSpec defines the desired state of MySQLUser
type MySQLUserSpec struct {

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Cluster name is immutable"

	// Cluster name to reference to, which decides the destination
	ClusterName string `json:"clusterName"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Username is immutable"
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_]*$`
	// +kubebuilder:validation:MaxLength=64

	// Username
	Username string `json:"username"`

	// +kubebuilder:default=%
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Host is immutable"
	// +kubebuilder:validation:Pattern=`^(\*|%|[a-zA-Z0-9._-]+|\d{1,3}(\.\d{1,3}){3})$`

	// Host address where the client connects, default to '%'
	Host string `json:"host"`

	// +kubebuilder:validation:Required
	// Secret to reference to, which contains the password
	PasswordSecretRef SecretRef `json:"passwordSecretRef"`

	// Grants of database user. If specified (even if empty), grants will be managed.
	// If not specified, grants will not be managed by the operator.
	Grants *[]Grant `json:"grants,omitempty"`

	// Properties of database user (list of name-value pairs for SET PROPERTY commands).
	// If specified (even if empty), properties will be managed.
	// If not specified, properties will not be managed by the operator.
	Properties *[]Property `json:"properties,omitempty"`
}

// MySQLUserStatus defines the observed state of MySQLUser
type MySQLUserStatus struct {

	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	Phase      string             `json:"phase,omitempty"`
	Reason     string             `json:"reason,omitempty"`

	// +kubebuilder:default=false

	// true if user is created
	UserCreated bool `json:"userCreated,omitempty"`
}

func (m *MySQLUser) GetConditions() []metav1.Condition {
	return m.Status.Conditions
}

func (m *MySQLUser) SetConditions(conditions []metav1.Condition) {
	m.Status.Conditions = conditions
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Created",type="boolean",JSONPath=".status.userCreated"

// MySQLUser is the Schema for the mysqlusers API
type MySQLUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLUserSpec   `json:"spec,omitempty"`
	Status MySQLUserStatus `json:"status,omitempty"`
}

func (u MySQLUser) GetUserIdentity() string {
	return fmt.Sprintf("'%s'@'%s'", u.Spec.Username, u.Spec.Host)
}

//+kubebuilder:object:root=true

// MySQLUserList contains a list of MySQLUser
type MySQLUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQLUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQLUser{}, &MySQLUserList{})
}
