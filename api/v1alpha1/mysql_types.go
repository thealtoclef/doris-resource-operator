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

// MySQLSpec holds the connection information for the target MySQL cluster.
type MySQLSpec struct {

	// Host is MySQL host of target MySQL cluster.
	Host string `json:"host"`

	//+kubebuilder:default=3306

	// Port is MySQL port of target MySQL cluster.
	Port int16 `json:"port,omitempty"`

	// AuthSecret is a reference to a kubernetes basic auth secret containing
	// username and password keys for authenticating to the client.
	// +kubebuilder:validation:Required
	AuthSecret string `json:"authSecret"`
}

// MySQLStatus defines the observed state of MySQL
type MySQLStatus struct {
	// true if successfully connected to the MySQL cluster
	Connected bool `json:"connected,omitempty"`

	// Reason for connection failure
	Reason string `json:"reason,omitempty"`

	//+kubebuilder:default=0

	// The number of users in this MySQL
	UserCount int32 `json:"userCount"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".metadata.name"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Connected",type=boolean,JSONPath=".status.connected"
// +kubebuilder:printcolumn:name="Host",type=string,JSONPath=".spec.host"
// +kubebuilder:printcolumn:name="AuthSecret",type=string,JSONPath=".spec.authSecret"
// +kubebuilder:printcolumn:name="UserCount",type="integer",JSONPath=".status.userCount"

// MySQL is the Schema for the mysqls API
type MySQL struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLSpec   `json:"spec,omitempty"`
	Status MySQLStatus `json:"status,omitempty"`
}

func (m MySQL) GetKey() string {
	return fmt.Sprintf("%s-%s", m.Namespace, m.Name)
}

//+kubebuilder:object:root=true

// MySQLList contains a list of MySQL
type MySQLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQL `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQL{}, &MySQLList{})
}
