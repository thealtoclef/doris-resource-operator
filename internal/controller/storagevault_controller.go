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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	_ "github.com/go-sql-driver/mysql"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	metrics "github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
)

const (
	storageVaultFinalizer                   = "storagevault.nakamasato.com/finalizer"
	storageVaultReasonCompleted             = "Successfully reconciled"
	storageVaultReasonMySQLConnectionFailed = "Failed to connect to cluster"
	storageVaultReasonFailedToCreateVault   = "Failed to create storage vault"
	storageVaultReasonFailedToGetSecret     = "Failed to get Secret"
	storageVaultReasonFailedToDropVault     = "Failed to drop storage vault"
	storageVaultPhaseReady                  = "Ready"
	storageVaultPhaseNotReady               = "NotReady"
	storageVaultReasonFailedToFinalize      = "Failed to finalize"
	storageVaultReasonMySQLFetchFailed      = "Failed to fetch cluster"

	// S3 Secret keys
	s3AccessKeyField = "access_key"
	s3SecretKeyField = "secret_key"
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
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found")

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
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonMySQLConnectionFailed
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

	// Finalize if DeletionTimestamp exists
	if !storageVault.GetDeletionTimestamp().IsZero() {
		log.Info("Resource marked for deletion")
		if controllerutil.ContainsFinalizer(storageVault, storageVaultFinalizer) {
			log.Info("ContainsFinalizer is true")
			// Run finalization logic
			if err := r.finalizeStorageVault(ctx, mysqlClient, storageVault); err != nil {
				log.Error(err, "Failed to finalize")
				storageVault.Status.Phase = storageVaultPhaseNotReady
				storageVault.Status.Reason = storageVaultReasonFailedToFinalize
				if serr := r.Status().Update(ctx, storageVault); serr != nil {
					log.Error(serr, "Failed to update finalization status")
				}
				return ctrl.Result{}, err
			}
			log.Info("Finalization completed")

			// Remove finalizer
			if controllerutil.RemoveFinalizer(storageVault, storageVaultFinalizer) {
				log.Info("Removing finalizer")
				err := r.Update(ctx, storageVault)
				if err != nil {
					log.Error(err, "Failed to remove finalizer")
					return ctrl.Result{}, err
				}
				log.Info("Finalizer removed")
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil // should return success when not having the finalizer
	}

	// Add finalizer if not exists
	log.Info("Add Finalizer for this CR")
	if controllerutil.AddFinalizer(storageVault, storageVaultFinalizer) {
		log.Info("Added Finalizer")
		err = r.Update(ctx, storageVault)
		if err != nil {
			log.Info("Failed to update after adding finalizer")
			return ctrl.Result{}, err // requeue
		}
		log.Info("Updated successfully after adding finalizer")
	} else {
		log.Info("Already has finalizer")
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. StorageVault cannot be created.", "mysql", mysql.Name, "storageVault", storageVault.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Check if storage vault exists
	exists, err := r.storageVaultExists(ctx, mysqlClient, storageVault.Spec.Name)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to check if storage vault exists", "clusterName", clusterName, "storageVaultName", storageVault.Spec.Name)
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonFailedToCreateVault
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Create storage vault if not exists
		if err := r.createStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "Failed to create storage vault")
			storageVault.Status.Phase = storageVaultPhaseNotReady
			storageVault.Status.Reason = storageVaultReasonFailedToCreateVault
			if serr := r.Status().Update(ctx, storageVault); serr != nil {
				log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
		log.Info("Created storage vault successfully")
		storageVault.Status.VaultCreated = true
		metrics.StorageVaultCreatedTotal.Increment()
	} else {
		storageVault.Status.VaultCreated = true
		// Update storage vault if exists
		if err := r.updateStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "Failed to update storage vault")
			storageVault.Status.Phase = storageVaultPhaseNotReady
			storageVault.Status.Reason = storageVaultReasonFailedToCreateVault
			if serr := r.Status().Update(ctx, storageVault); serr != nil {
				log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
		log.Info("Updated storage vault successfully")
	}

	// Handle default vault setting
	if err := r.handleDefaultVault(ctx, mysqlClient, storageVault); err != nil {
		log.Error(err, "Failed to handle default storage vault")
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonFailedToCreateVault
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	// Update status
	storageVault.Status.Phase = storageVaultPhaseReady
	storageVault.Status.Reason = storageVaultReasonCompleted
	if serr := r.Status().Update(ctx, storageVault); serr != nil {
		log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
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
			return false, fmt.Errorf("failed to scan storage vault row: %w", err)
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

	// Initialize properties map
	properties := make(map[string]string)

	// Set the vault type
	properties["type"] = string(storageVault.Spec.Type)

	// Set properties based on the vault type
	switch storageVault.Spec.Type {
	case mysqlv1alpha1.S3VaultType:
		// Add S3 properties
		s3Props := storageVault.Spec.S3Properties
		properties["s3.endpoint"] = s3Props.Endpoint
		properties["s3.region"] = s3Props.Region
		properties["s3.root.path"] = s3Props.RootPath
		properties["s3.bucket"] = s3Props.Bucket
		properties["provider"] = string(s3Props.Provider)

		if s3Props.UsePathStyle != nil {
			properties["use_path_style"] = fmt.Sprintf("%t", *s3Props.UsePathStyle)
		}

		// Get access_key and secret_key from the secret
		secret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: storageVault.Namespace,
			Name:      s3Props.AuthSecret,
		}, secret)
		if err != nil {
			log.Error(err, "Failed to get auth secret", "secretName", s3Props.AuthSecret)
			return fmt.Errorf("failed to get auth secret: %w", err)
		}

		// Get access_key and secret_key
		accessKey, ok := secret.Data[s3AccessKeyField]
		if !ok {
			return fmt.Errorf("auth secret missing required field: %s", s3AccessKeyField)
		}
		secretKey, ok := secret.Data[s3SecretKeyField]
		if !ok {
			return fmt.Errorf("auth secret missing required field: %s", s3SecretKeyField)
		}

		properties["s3.access_key"] = string(accessKey)
		properties["s3.secret_key"] = string(secretKey)

	case mysqlv1alpha1.HDFSVaultType:
		// HDFS support to be implemented in the future
		return fmt.Errorf("HDFS vault type is not currently supported")

	default:
		return fmt.Errorf("unsupported vault type: %s", storageVault.Spec.Type)
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	query := fmt.Sprintf("CREATE STORAGE VAULT IF NOT EXISTS %s PROPERTIES (%s)",
		storageVault.Spec.Name,
		strings.Join(props, ", "))

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to create storage vault")
		return err
	}

	log.Info("Storage vault created successfully")
	return nil
}

// updateStorageVault updates an existing storage vault in Doris
func (r *StorageVaultReconciler) updateStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Updating storage vault")

	// Get current vault properties to compare
	currentProperties, err := r.getStorageVaultProperties(ctx, db, storageVault.Spec.Name)
	if err != nil {
		return fmt.Errorf("failed to get current storage vault properties: %w", err)
	}

	// Initialize properties map for the update
	properties := make(map[string]string)

	// Type is required in the ALTER command even though it's immutable
	vaultType := string(storageVault.Spec.Type)
	properties["type"] = vaultType

	// Check if the vault name is changed
	if storageVault.Spec.Name != storageVault.Name {
		properties["VAULT_NAME"] = storageVault.Spec.Name
	}

	// Set properties based on the vault type
	switch mysqlv1alpha1.VaultType(vaultType) {
	case mysqlv1alpha1.S3VaultType:
		// Add S3 properties that can be modified
		s3Props := storageVault.Spec.S3Properties
		if s3Props.UsePathStyle != nil {
			properties["use_path_style"] = fmt.Sprintf("%t", *s3Props.UsePathStyle)
		}

		// Get access_key and secret_key from the secret
		secret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: storageVault.Namespace,
			Name:      s3Props.AuthSecret,
		}, secret)
		if err != nil {
			log.Error(err, "Failed to get auth secret", "secretName", s3Props.AuthSecret)
			return fmt.Errorf("failed to get auth secret: %w", err)
		}

		// Get access_key from secret
		accessKey, ok := secret.Data[s3AccessKeyField]
		if !ok {
			return fmt.Errorf("auth secret missing required field: %s", s3AccessKeyField)
		}

		// Get secret_key from secret
		secretKey, ok := secret.Data[s3SecretKeyField]
		if !ok {
			return fmt.Errorf("auth secret missing required field: %s", s3SecretKeyField)
		}

		// Compare access key with current (if available)
		currentAccessKey, hasCurrentAccessKey := currentProperties["s3.access_key"]
		if !hasCurrentAccessKey || currentAccessKey != string(accessKey) {
			log.Info("Access key has changed, updating both access key and secret key")
			properties["s3.access_key"] = string(accessKey)
			properties["s3.secret_key"] = string(secretKey)
		} else {
			log.Info("Access key unchanged, assuming secret key also unchanged")
		}

	case mysqlv1alpha1.HDFSVaultType:
		// HDFS support to be implemented in the future
		return fmt.Errorf("HDFS vault type is not currently supported")

	default:
		return fmt.Errorf("unsupported vault type: %s", vaultType)
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	// If no properties to update, skip the update
	if len(props) == 0 {
		log.Info("No properties to update")
		return nil
	}

	query := fmt.Sprintf("ALTER STORAGE VAULT %s PROPERTIES (%s)",
		storageVault.Spec.Name,
		strings.Join(props, ", "))

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to update storage vault")
		return err
	}

	log.Info("Storage vault updated successfully")
	return nil
}

// getStorageVaultProperties retrieves the current properties of a storage vault
func (r *StorageVaultReconciler) getStorageVaultProperties(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", name)
	log.Info("Getting storage vault properties")

	// Execute SHOW STORAGE VAULTS to get all vaults
	rows, err := db.QueryContext(ctx, "SHOW STORAGE VAULTS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Find the target vault and parse its properties
	for rows.Next() {
		var vaultName, id, propertiesStr, isDefault string
		if err := rows.Scan(&vaultName, &id, &propertiesStr, &isDefault); err != nil {
			return nil, fmt.Errorf("failed to scan storage vault row: %w", err)
		}

		if vaultName == name {
			// Parse the properties string
			properties := make(map[string]string)

			// More robust parsing of key-value pairs
			// Properties are typically in format "key: value"
			pairs := strings.Split(propertiesStr, ", ")
			for _, pair := range pairs {
				parts := strings.SplitN(pair, ": ", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					// Remove quotes if present
					value = strings.Trim(value, "\"")
					properties[key] = value
				}
			}

			// Add isDefault property
			properties["isDefault"] = isDefault

			return properties, nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("storage vault not found: %s", name)
}

// handleDefaultVault handles setting the default storage vault
func (r *StorageVaultReconciler) handleDefaultVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	// Get current default vault
	var currentDefault string
	err := db.QueryRowContext(ctx, "SHOW DEFAULT STORAGE VAULT").Scan(&currentDefault)
	if err != nil {
		return fmt.Errorf("failed to get current default storage vault: %w", err)
	}

	log.Info("Current default storage vault", "currentDefault", currentDefault)

	// Update status
	storageVault.Status.IsDefault = currentDefault == storageVault.Spec.Name

	// If this vault should be default and isn't currently default
	if storageVault.Spec.IsDefault != nil && *storageVault.Spec.IsDefault && currentDefault != storageVault.Spec.Name {
		log.Info("Setting storage vault as default")
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET DEFAULT STORAGE VAULT %s", storageVault.Spec.Name))
		if err != nil {
			return fmt.Errorf("failed to set default storage vault: %w", err)
		}
		storageVault.Status.IsDefault = true
		log.Info("Successfully set storage vault as default")
	}

	return nil
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
