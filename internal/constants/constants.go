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
	"github.com/nakamasato/mysql-operator/internal/utils"
)

// ReconciliationPeriod is the time between controller reconciliations.
// It's initialized from the utils.GetReconciliationPeriod() function which reads from
// the RECONCILIATION_PERIOD_SECONDS environment variable.
// The value is initialized once when the package is loaded to avoid repeated environment lookups.
var ReconciliationPeriod = utils.GetReconciliationPeriod()

// Common status phases
const (
	PhaseReady    = "Ready"
	PhaseNotReady = "NotReady"
)

// Common reason constants
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

	// Catalog-specific reason constants
	ReasonFailedToCreateCatalog = "Failed to create catalog"
	ReasonFailedToDropCatalog   = "Failed to drop catalog"

	// StorageVault-specific reason constants
	ReasonFailedToCreateVault   = "Failed to create vault"
	ReasonFailedToUpdateVault   = "Failed to update vault"
	ReasonMultipleDefaultVaults = "Multiple default storage vaults defined - only one allowed"

	// WorkloadGroup-specific reason constants
	ReasonFailedToCreateWorkloadGroup = "Failed to create workload group"
	ReasonFailedToDropWorkloadGroup   = "Failed to drop workload group"
	ReasonFailedToUpdateWorkloadGroup = "Failed to update workload group"
)

// Annotation constants
const (
	StorageVaultLastKnownNameAnnotation = "mysql.nakamasato.com/last-known-vault-name"
	CatalogLastKnownNameAnnotation      = "mysql.nakamasato.com/last-known-catalog-name"
)
