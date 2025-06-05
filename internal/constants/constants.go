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

package constants

import (
	"os"
	"strconv"
	"time"
)

// Default reconciliation period in seconds
const DefaultReconciliationPeriodSeconds = 60

// GetReconciliationPeriod returns the reconciliation period from environment variable
// "RECONCILIATION_PERIOD_SECONDS" or defaults to 60 seconds if not set or invalid.
func GetReconciliationPeriod() time.Duration {
	envPeriod := os.Getenv("RECONCILIATION_PERIOD_SECONDS")
	if envPeriod != "" {
		if seconds, err := strconv.Atoi(envPeriod); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return DefaultReconciliationPeriodSeconds * time.Second
}

// ReconciliationPeriod is the time between controller reconciliations.
// It's initialized from the GetReconciliationPeriod() function which reads from
// the RECONCILIATION_PERIOD_SECONDS environment variable.
// The value is initialized once when the package is loaded to avoid repeated environment lookups.
var ReconciliationPeriod = GetReconciliationPeriod()

// Status phases
const (
	PhaseReady    = "Ready"
	PhaseNotReady = "NotReady"
)

// Reason constants
const (
	// Success reasons
	ReasonCompleted = "Successfully reconciled"

	// Error reasons
	ReasonFailedToFinalize      = "Failed to finalize"
	ReasonMySQLFetchFailed      = "Failed to fetch cluster"
	ReasonMySQLConnectionFailed = "Failed to connect to cluster"
	ReasonFailedToGetSecret     = "Failed to get Secret"

	// MySQLUser-specific reason constants
	ReasonMySQLFailedToCreateUser     = "Failed to create user"
	ReasonMySQLFailedToUpdatePassword = "Failed to update password"
	ReasonMySQLFailedToGrant          = "Failed to grant"
	ReasonMySQLFailedToSetProperty    = "Failed to set property"

	// Catalog-specific reason constants
	ReasonFailedToFetchCatalog  = "Failed to fetch catalog"
	ReasonFailedToCreateCatalog = "Failed to create catalog"
	ReasonFailedToDropCatalog   = "Failed to drop catalog"

	// StorageVault-specific reason constants
	ReasonFailedToFetchStorageVault  = "Failed to fetch storage vault"
	ReasonFailedToCreateStorageVault = "Failed to create storage vault"
	ReasonFailedToUpdateStorageVault = "Failed to update storage vault"

	// WorkloadGroup-specific reason constants
	ReasonFailedToFetchWorkloadGroup  = "Failed to fetch workload group"
	ReasonFailedToCreateWorkloadGroup = "Failed to create workload group"
	ReasonFailedToDropWorkloadGroup   = "Failed to drop workload group"
	ReasonFailedToUpdateWorkloadGroup = "Failed to update workload group"

	// GlobalVariable-specific reason constants
	ReasonFailedToFetchVariable  = "Failed to fetch variable"
	ReasonFailedToUpdateVariable = "Failed to update variable"
	ReasonVariableNotExist       = "Variable does not exist in the database"
)

// Annotation constants
const (
	DeletionPolicyAnnotation            = "mysql.nakamasato.com/deletion-policy"
	StorageVaultLastKnownNameAnnotation = "mysql.nakamasato.com/last-known-vault-name"
	CatalogLastKnownNameAnnotation      = "mysql.nakamasato.com/last-known-catalog-name"
)
