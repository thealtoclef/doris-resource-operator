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

package controllers

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	_ "github.com/go-sql-driver/mysql"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	metrics "github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	storageVaultFinalizer = "storagevault.nakamasato.com/finalizer"

	// S3 Secret keys
	s3AccessKeyField = "access_key"
	s3SecretKeyField = "secret_key"
)

// VaultPropertyMode defines whether we're creating or updating a vault
type VaultPropertyMode int

const (
	// PropertyModeCreate is used when creating a new vault
	PropertyModeCreate VaultPropertyMode = iota
	// PropertyModeUpdate is used when updating an existing vault
	PropertyModeUpdate
)

// StorageVaultProperties encapsulates property handling for different vault types
type StorageVaultProperties struct {
	// General properties
	Type       string
	VaultName  string
	Properties map[string]string
}

// FieldMapping defines mapping between CR fields and SQL properties
type FieldMapping struct {
	// CRToSQL maps from CR field names to SQL property names
	CRToSQL map[string]string
	// SQLToCR maps from SQL property names (as in SHOW output) to CR field names
	SQLToCR map[string]string
	// Mutable indicates which fields can be changed
	Mutable map[string]bool
}

// Define field mappings for each vault type
var (
	// S3FieldMapping maps S3 properties between CR and SQL
	S3FieldMapping = FieldMapping{
		CRToSQL: map[string]string{
			"endpoint":     "s3.endpoint",
			"region":       "s3.region",
			"rootPath":     "s3.root.path",
			"bucket":       "s3.bucket",
			"provider":     "provider",
			"usePathStyle": "use_path_style",
			"accessKey":    "s3.access_key",
			"secretKey":    "s3.secret_key",
		},
		SQLToCR: map[string]string{
			"ak":             "accessKey",
			"sk":             "secretKey",
			"bucket":         "bucket",
			"prefix":         "rootPath",
			"endpoint":       "endpoint",
			"region":         "region",
			"provider":       "provider",
			"use_path_style": "usePathStyle",
		},
		Mutable: map[string]bool{
			"usePathStyle": true,
			"accessKey":    true,
			"secretKey":    true,
			// All other fields are immutable
		},
	}
)

// StorageVaultReconciler reconciles a StorageVault object
type StorageVaultReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
}

// NewStorageVaultReconciler creates a new StorageVaultReconciler
func NewStorageVaultReconciler(client client.Client, scheme *runtime.Scheme, mysqlClients mysqlinternal.MySQLClients) *StorageVaultReconciler {
	return &StorageVaultReconciler{
		Client:       client,
		Scheme:       scheme,
		MySQLClients: mysqlClients,
	}
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=storagevaults,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=storagevaults/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=storagevaults/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StorageVaultReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")

	// Fetch StorageVault
	storageVault := &mysqlv1alpha1.StorageVault{}
	err := r.Get(ctx, req.NamespacedName, storageVault)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchStorageVault] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchStorageVault] Failed")
		return ctrl.Result{}, err
	}
	log.Info("[FetchStorageVault] Found", "name", storageVault.Name, "namespace", storageVault.Namespace)
	clusterName := storageVault.Spec.ClusterName

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		storageVault.Status.Phase = constants.PhaseNotReady
		storageVault.Status.Reason = constants.ReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found")

	// If this vault wants to be default, check for other default vaults
	if storageVault.Spec.IsDefault != nil && *storageVault.Spec.IsDefault {
		// Check if there are other StorageVaults with IsDefault=true for the same cluster
		storageVaultList := &mysqlv1alpha1.StorageVaultList{}
		if err := r.List(ctx, storageVaultList, &client.ListOptions{Namespace: req.Namespace}); err != nil {
			log.Error(err, "Failed to list StorageVaults")
			return ctrl.Result{}, err
		}

		// Look for other default vaults in the same cluster
		for _, v := range storageVaultList.Items {
			// Skip the current vault
			if v.Name == storageVault.Name {
				continue
			}

			// Skip vaults from other clusters
			if v.Spec.ClusterName != storageVault.Spec.ClusterName {
				continue
			}

			// If another vault is marked as default, raise an error
			if v.Spec.IsDefault != nil && *v.Spec.IsDefault {
				log.Error(nil, "Multiple StorageVaults marked as default",
					"current", storageVault.Name, "other", v.Name)

				storageVault.Status.Phase = constants.PhaseNotReady
				storageVault.Status.Reason = constants.ReasonMultipleDefaultVaults
				if serr := r.Status().Update(ctx, storageVault); serr != nil {
					log.Error(serr, "Failed to update StorageVault status")
				}
				return ctrl.Result{}, fmt.Errorf("conflict: multiple StorageVaults marked as default for cluster %s: %s and %s",
					clusterName, storageVault.Name, v.Name)
			}
		}
	}

	// SetOwnerReference if not exists
	if !r.ifOwnerReferencesContains(storageVault.OwnerReferences, mysql) {
		err := controllerutil.SetControllerReference(mysql, storageVault, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
		err = r.Update(ctx, storageVault)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		storageVault.Status.Phase = constants.PhaseNotReady
		storageVault.Status.Reason = constants.ReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		// If MySQL client not found, requeue with a delay to wait for MySQL controller to initialize it
		if err == mysqlinternal.ErrMySQLClientNotFound {
			log.Info("[MySQLClient] MySQL client not found, waiting for MySQL controller to initialize", "key", mysql.GetKey())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		// For other errors, requeue immediately
		return ctrl.Result{}, err
	}
	log.Info("[MySQLClient] Successfully connected")

	// Handle finalizer
	finalizerResult, finalizerErr := utils.HandleFinalizer(utils.FinalizerParams{
		Object:    storageVault,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: storageVaultFinalizer,
		FinalizationFunc: func() error {
			return r.finalizeStorageVault(ctx, mysqlClient, storageVault)
		},
		OnFailure: func(err error) error {
			storageVault.Status.Phase = constants.PhaseNotReady
			storageVault.Status.Reason = constants.ReasonFailedToFinalize
			if serr := r.Status().Update(ctx, storageVault); serr != nil {
				log.Error(serr, "Failed to update finalization status")
				return serr
			}
			return nil
		},
	})

	if finalizerErr != nil || !storageVault.GetDeletionTimestamp().IsZero() {
		// If finalizer processing returned an error or object is being deleted,
		// return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. StorageVault cannot be created.", "mysql", mysql.Name, "storageVault", storageVault.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Get the last known vault name that was successfully created/updated in Doris
	vaultNameInDoris := r.getLastKnownVaultName(storageVault)
	log.Info("Vault name tracking", "specName", storageVault.Spec.Name, "lastKnownName", vaultNameInDoris)

	// Check if storage vault exists - use the last known name to check
	exists, err := r.storageVaultExists(ctx, mysqlClient, vaultNameInDoris)
	if err != nil {
		log.Error(err, "Failed to check if storage vault exists", "clusterName", clusterName, "storageVaultName", vaultNameInDoris)
		storageVault.Status.Phase = constants.PhaseNotReady
		storageVault.Status.Reason = constants.ReasonFailedToCreateVault
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Create storage vault
		if err := r.createStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "Failed to create storage vault")
			storageVault.Status.Phase = constants.PhaseNotReady
			storageVault.Status.Reason = constants.ReasonFailedToCreateVault
			if serr := r.Status().Update(ctx, storageVault); serr != nil {
				log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
		storageVault.Status.VaultCreated = true
		metrics.StorageVaultCreatedTotal.Increment()
	} else {
		storageVault.Status.VaultCreated = true
		// Update storage vault
		if err := r.updateStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "Failed to update storage vault")
			storageVault.Status.Phase = constants.PhaseNotReady
			storageVault.Status.Reason = constants.ReasonFailedToUpdateVault
			if serr := r.Status().Update(ctx, storageVault); serr != nil {
				log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
	}

	// Update status
	storageVault.Status.Phase = constants.PhaseReady
	storageVault.Status.Reason = constants.ReasonCompleted

	// Update the last known vault name annotation for successful operations
	if storageVault.Status.VaultCreated {
		r.updateLastKnownVaultName(storageVault)
	}

	// Save all changes - both status and annotations
	if err := r.Update(ctx, storageVault); err != nil {
		log.Error(err, "Failed to update StorageVault resource", "storageVault", storageVault.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Also update status separately in case the above update didn't apply status changes
	if serr := r.Status().Update(ctx, storageVault); serr != nil {
		log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StorageVaultReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.StorageVault{}).
		Complete(r)
}

// finalizeStorageVault drops the storage vault from Doris
func (r *StorageVaultReconciler) finalizeStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Finalization requested")

	if !storageVault.Status.VaultCreated {
		log.Info("Storage vault was not created, skipping finalization")
		return nil
	}

	// StorageVault deletion is not currently supported
	return fmt.Errorf("storage vault deletion is not currently supported")
}

// storageVaultExists checks if a storage vault exists
func (r *StorageVaultReconciler) storageVaultExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", name)
	log.Info("Checking if storage vault exists")

	// Execute SHOW STORAGE VAULTS to get all vaults
	rows, err := db.QueryContext(ctx, "SHOW STORAGE VAULTS")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	// Parse columns based on SHOW STORAGE VAULTS output format
	exists := false
	for rows.Next() {
		var vaultName, id, properties, isDefault string
		if err := rows.Scan(&vaultName, &id, &properties, &isDefault); err != nil {
			log.Error(err, "Failed to scan storage vault row")
			return false, err
		}
		if vaultName == name {
			exists = true
			break
		}
	}

	if err := rows.Err(); err != nil {
		return false, err
	}

	log.Info("Storage vault check result", "exists", exists)
	return exists, nil
}

// createStorageVault creates a new storage vault in Doris
func (r *StorageVaultReconciler) createStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Creating new storage vault")

	// Get properties for SQL
	properties, err := r.crToSQL(ctx, storageVault, PropertyModeCreate, nil)
	if err != nil {
		log.Error(err, "Failed to get properties for creation")
		return err
	}

	// Build and execute the creation query
	propsStr := r.propertiesToSQLString(properties)
	query := fmt.Sprintf("CREATE STORAGE VAULT IF NOT EXISTS %s PROPERTIES (%s)",
		storageVault.Spec.Name, propsStr)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute CREATE STORAGE VAULT query")
		return err
	}

	// Handle setting default status if specified
	if storageVault.Spec.IsDefault != nil && *storageVault.Spec.IsDefault {
		log.Info("Setting storage vault as default")
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET %s AS DEFAULT STORAGE VAULT", storageVault.Spec.Name))
		if err != nil {
			log.Error(err, "Failed to execute SET AS DEFAULT STORAGE VAULT query")
			return err
		}
		storageVault.Status.IsDefault = true
		log.Info("Successfully set storage vault as default")
	} else {
		// If not explicitly set as default, update the status with current default status (false for a new vault)
		storageVault.Status.IsDefault = false
	}

	log.Info("Storage vault created successfully")

	// Update the last known vault name after successful creation
	r.updateLastKnownVaultName(storageVault)

	return nil
}

// updateStorageVault updates an existing storage vault in Doris
func (r *StorageVaultReconciler) updateStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Checking if storage vault needs updating")

	// Get the last known vault name that exists in Doris
	lastKnownName := r.getLastKnownVaultName(storageVault)

	// Get current vault properties using the last known name
	currentProperties, err := r.getStorageVaultProperties(ctx, db, lastKnownName)
	if err != nil {
		log.Error(err, "Failed to get current storage vault properties")
		return err
	}

	// Check if this vault is currently set as default
	isCurrentlyDefault := false
	if defaultVal, exists := currentProperties["isDefault"]; exists {
		isCurrentlyDefault = strings.ToUpper(defaultVal) == "TRUE"
	}

	// Update the status with the current default information
	storageVault.Status.IsDefault = isCurrentlyDefault

	// Get desired properties for SQL update based on the CR
	desiredProperties, err := r.crToSQL(ctx, storageVault, PropertyModeUpdate, currentProperties)
	if err != nil {
		log.Error(err, "Failed to get properties for update")
		return err
	}

	// Compare current and desired properties to determine what needs to change
	propsToUpdate := make(map[string]string)

	// Add the type property which is required in the SQL statement
	propsToUpdate["type"] = desiredProperties["type"]

	// Compare and add only changed properties
	for k, v := range desiredProperties {
		if k == "type" {
			continue // Already added above
		}

		// Check if property value has changed
		if currentVal, exists := currentProperties[k]; !exists || currentVal != v {
			propsToUpdate[k] = v
		}
	}

	// Special case: VAULT_NAME needs to be included if name is changing
	if vaultName, exists := desiredProperties["VAULT_NAME"]; exists {
		propsToUpdate["VAULT_NAME"] = vaultName
	}

	// If there are properties to update besides just "type", execute the update
	if len(propsToUpdate) > 1 {
		log.Info("Updating storage vault properties", "count", len(propsToUpdate)-1)
		// Build and execute the update query - use the last known name for the ALTER command
		propsStr := r.propertiesToSQLString(propsToUpdate)
		query := fmt.Sprintf("ALTER STORAGE VAULT %s PROPERTIES (%s)",
			lastKnownName, propsStr)

		_, err = db.ExecContext(ctx, query)
		if err != nil {
			log.Error(err, "Failed to execute ALTER STORAGE VAULT query")
			return err
		}
		log.Info("Storage vault properties updated successfully", "count", len(propsToUpdate)-1)
	} else {
		log.Info("Storage vault properties unchanged")
	}

	// Handle default status change separately from property updates
	if storageVault.Spec.IsDefault != nil {
		wantsToBeDefault := *storageVault.Spec.IsDefault

		// Only take action if the desired default state is different from current
		if wantsToBeDefault != isCurrentlyDefault {
			if wantsToBeDefault {
				// Set this vault as default
				log.Info("Setting storage vault as default")
				_, err := db.ExecContext(ctx, fmt.Sprintf("SET %s AS DEFAULT STORAGE VAULT", lastKnownName))
				if err != nil {
					log.Error(err, "Failed to execute SET AS DEFAULT STORAGE VAULT query")
					return err
				}

				// Update status after successful change
				storageVault.Status.IsDefault = true
				log.Info("Storage vault is now set as default")
			} else {
				// Want to unset default status
				// First check if there's another default vault
				storageVaultList := &mysqlv1alpha1.StorageVaultList{}
				if err := r.List(ctx, storageVaultList, &client.ListOptions{Namespace: storageVault.Namespace}); err != nil {
					log.Error(err, "Failed to list StorageVaults")
					return err
				}

				otherVaultMarkedDefault := false
				for _, v := range storageVaultList.Items {
					// Skip this vault
					if v.Name == storageVault.Name {
						continue
					}

					// Skip vaults from other clusters
					if v.Spec.ClusterName != storageVault.Spec.ClusterName {
						continue
					}

					// Check if another vault wants to be default
					if v.Spec.IsDefault != nil && *v.Spec.IsDefault {
						otherVaultMarkedDefault = true
						break
					}
				}

				if !otherVaultMarkedDefault {
					// No other vault is marked as default, so we need to explicitly unset this one
					log.Info("No other vault marked as default, explicitly unsetting this vault")
					_, err := db.ExecContext(ctx, "UNSET DEFAULT STORAGE VAULT")
					if err != nil {
						log.Error(err, "Failed to execute UNSET DEFAULT STORAGE VAULT query")
						return err
					}
				}

				// Update the status regardless of whether we ran the UNSET command
				// If another vault was marked default, setting that one will unset this one
				storageVault.Status.IsDefault = false
				log.Info("Storage vault is no longer default")
			}
		} else {
			log.Info("Storage vault default status unchanged")
		}
	}

	// Update the last known name after successful update
	r.updateLastKnownVaultName(storageVault)

	return nil
}

// crToSQL converts CR properties to SQL format using the field mapping
func (r *StorageVaultReconciler) crToSQL(ctx context.Context, storageVault *mysqlv1alpha1.StorageVault, mode VaultPropertyMode, currentProperties map[string]string) (map[string]string, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	properties := make(map[string]string)
	properties["type"] = string(storageVault.Spec.Type)

	var mapping FieldMapping

	switch storageVault.Spec.Type {
	case mysqlv1alpha1.S3VaultType:
		if storageVault.Spec.S3Properties == nil {
			return nil, fmt.Errorf("S3Properties is nil")
		}

		mapping = S3FieldMapping
		s3Props := storageVault.Spec.S3Properties

		// Function to add field if needed for current mode
		addField := func(crField string, value string) {
			sqlField, exists := mapping.CRToSQL[crField]
			if !exists {
				return // Skip if no mapping exists
			}

			// For create mode, add all fields
			if mode == PropertyModeCreate {
				properties[sqlField] = value
				return
			}

			// For update mode, only add mutable fields
			if isMutable, ok := mapping.Mutable[crField]; ok && isMutable {
				properties[sqlField] = value
			}
		}

		// Add all the mapped fields from the S3Properties
		addField("endpoint", s3Props.Endpoint)
		addField("region", s3Props.Region)
		addField("rootPath", s3Props.RootPath)
		addField("bucket", s3Props.Bucket)
		addField("provider", string(s3Props.Provider))

		// Add usePathStyle if specified
		if s3Props.UsePathStyle != nil {
			addField("usePathStyle", fmt.Sprintf("%t", *s3Props.UsePathStyle))
		}

		// Get access_key and secret_key from the secret
		secret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: storageVault.Namespace,
			Name:      s3Props.AuthSecret,
		}, secret)
		if err != nil {
			log.Error(err, "Failed to get auth secret", "secretName", s3Props.AuthSecret)
			return nil, err
		}

		// Extract access_key
		accessKey, ok := secret.Data[s3AccessKeyField]
		if !ok {
			return nil, fmt.Errorf("auth secret missing required field: %s", s3AccessKeyField)
		}

		// Extract secret_key
		secretKey, ok := secret.Data[s3SecretKeyField]
		if !ok {
			return nil, fmt.Errorf("auth secret missing required field: %s", s3SecretKeyField)
		}

		if mode == PropertyModeCreate {
			// For create, always add both
			addField("accessKey", string(accessKey))
			addField("secretKey", string(secretKey))
		} else if mode == PropertyModeUpdate {
			// For update, check if access key has changed
			currentAccessKey, hasCurrentAccessKey := currentProperties["s3.access_key"]
			if !hasCurrentAccessKey || currentAccessKey != string(accessKey) {
				log.Info("Access key has changed, updating both access key and secret key")
				addField("accessKey", string(accessKey))
				addField("secretKey", string(secretKey))
			} else {
				log.Info("Access key unchanged, assuming secret key also unchanged")
			}
		}

		// For updates, check if we need to change the vault name
		if mode == PropertyModeUpdate {
			// The desired vault name in Doris
			desiredVaultName := storageVault.Spec.Name

			// Get the actual current name from the database properties
			currentVaultName, ok := currentProperties["name"]
			if !ok {
				log.Info("Could not find current vault name in properties, skipping name check")
				// This should never happen since we set the name in parseStorageVaultRow
			} else {
				// Only add VAULT_NAME to the properties if the current vault name is different from the desired name
				if currentVaultName != desiredVaultName {
					log.Info("Vault name needs to change", "from", currentVaultName, "to", desiredVaultName)
					properties["VAULT_NAME"] = desiredVaultName
				}
			}
		}

	case mysqlv1alpha1.HDFSVaultType:
		// HDFS support to be implemented in the future
		return nil, fmt.Errorf("HDFS vault type is not currently supported")

	default:
		return nil, fmt.Errorf("unsupported vault type: %s", storageVault.Spec.Type)
	}

	return properties, nil
}

// getStorageVaultProperties retrieves the current properties of a storage vault
func (r *StorageVaultReconciler) getStorageVaultProperties(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", name)
	log.Info("Getting storage vault properties")

	// Execute SHOW STORAGE VAULTS to get all vaults
	rows, err := db.QueryContext(ctx, "SHOW STORAGE VAULTS")
	if err != nil {
		log.Error(err, "Failed to execute SHOW STORAGE VAULTS query")
		return nil, err
	}
	defer rows.Close()

	// Find the target vault and parse its properties
	for rows.Next() {
		var vaultName, id, propertiesStr, isDefault string
		if err := rows.Scan(&vaultName, &id, &propertiesStr, &isDefault); err != nil {
			log.Error(err, "Failed to scan storage vault row")
			return nil, err
		}

		if vaultName == name {
			// Parse the properties using the centralized mapping
			properties, err := r.parseStorageVaultRow(ctx, vaultName, id, propertiesStr, isDefault)
			if err != nil {
				return nil, err
			}
			return properties, nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("storage vault not found: %s", name)
}

// parseStorageVaultRow parses a row from SHOW STORAGE VAULTS using the field mapping
func (r *StorageVaultReconciler) parseStorageVaultRow(ctx context.Context, vaultName, id, propertiesStr, isDefault string) (map[string]string, error) {
	// Try to determine vault type from properties
	var mapping FieldMapping
	if strings.Contains(propertiesStr, "endpoint:") {
		mapping = S3FieldMapping
	} else {
		// Default to S3 mapping for now, extend as needed
		mapping = S3FieldMapping
	}

	// Parse the raw properties string
	rawProps := r.parsePropertiesString(propertiesStr)

	// Map the property keys using the mapping
	properties := make(map[string]string)
	for rawKey, value := range rawProps {
		// First, check if this is a field we know how to map
		if sqlKey, exists := mapping.SQLToCR[rawKey]; exists {
			// Map using the CR field name for consistency
			if crKey, exists := mapping.CRToSQL[sqlKey]; exists {
				properties[crKey] = value
			}
		} else {
			// For unmapped fields, keep the original key
			properties[rawKey] = value
		}
	}

	// Add type based on inference
	properties["type"] = string(mysqlv1alpha1.S3VaultType)

	// Add metadata
	properties["name"] = vaultName
	properties["id"] = id
	properties["isDefault"] = isDefault

	return properties, nil
}

// parsePropertiesString parses a properties string from SHOW STORAGE VAULTS
func (r *StorageVaultReconciler) parsePropertiesString(propertiesStr string) map[string]string {
	properties := make(map[string]string)

	// Regex for key-value pairs
	keyValueRegex := regexp.MustCompile(`(\w+):\s*("[^"]*"|[^\s]+)`)

	// Find all key-value pairs
	matches := keyValueRegex.FindAllStringSubmatch(propertiesStr, -1)
	for _, match := range matches {
		if len(match) == 3 {
			key := strings.TrimSpace(match[1])
			value := strings.TrimSpace(match[2])

			// Remove quotes if present
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}

			// Store all values as strings
			properties[key] = value
		}
	}

	return properties
}

// ifOwnerReferencesContains checks if the ownerReferences contains the MySQL object
func (r *StorageVaultReconciler) ifOwnerReferencesContains(ownerReferences []metav1.OwnerReference, mysql *mysqlv1alpha1.MySQL) bool {
	for _, ref := range ownerReferences {
		if ref.APIVersion == "mysql.nakamasato.com/v1alpha1" && ref.Kind == "MySQL" && ref.UID == mysql.UID {
			return true
		}
	}
	return false
}

// propertiesToSQLString converts a map of properties to a SQL string
func (r *StorageVaultReconciler) propertiesToSQLString(properties map[string]string) string {
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}
	return strings.Join(props, ", ")
}

// getLastKnownVaultName returns the last known vault name that was successfully created/updated
func (r *StorageVaultReconciler) getLastKnownVaultName(storageVault *mysqlv1alpha1.StorageVault) string {
	if name, ok := storageVault.Annotations[constants.StorageVaultLastKnownNameAnnotation]; ok && name != "" {
		return name
	}
	return storageVault.Spec.Name
}

// updateLastKnownVaultName sets the last known vault name annotation
func (r *StorageVaultReconciler) updateLastKnownVaultName(storageVault *mysqlv1alpha1.StorageVault) {
	if storageVault.Annotations == nil {
		storageVault.Annotations = make(map[string]string)
	}
	storageVault.Annotations[constants.StorageVaultLastKnownNameAnnotation] = storageVault.Spec.Name
}
