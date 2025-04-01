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
	"strconv"
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
	storageVaultReasonCompleted             = "Storage vault successfully reconciled"
	storageVaultReasonMySQLConnectionFailed = "Failed to connect to cluster"
	storageVaultReasonFailedToCreateVault   = "Failed to create storage vault"
	storageVaultReasonFailedToGetSecret     = "Failed to get Secret"
	storageVaultReasonFailedToDropVault     = "Failed to drop storage vault"
	storageVaultPhaseReady                  = "Ready"
	storageVaultPhaseNotReady               = "NotReady"
	storageVaultReasonFailedToFinalize      = "Failed to finalize storage vault"
	storageVaultReasonMySQLFetchFailed      = "Failed to fetch cluster"
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
		return ctrl.Result{}, err // requeue
	}
	log.Info("[MySQLClient] Successfully connected")

	// Finalize if DeletionTimestamp exists
	if !storageVault.GetDeletionTimestamp().IsZero() {
		log.Info("Resource marked for deletion")
		if controllerutil.ContainsFinalizer(storageVault, storageVaultFinalizer) {
			// Run finalization logic for storageVaultFinalizer
			if err := r.finalizeStorageVault(ctx, mysqlClient, storageVault); err != nil {
				log.Error(err, "Failed to finalize StorageVault")
				storageVault.Status.Phase = storageVaultPhaseNotReady
				storageVault.Status.Reason = storageVaultReasonFailedToFinalize
				if serr := r.Status().Update(ctx, storageVault); serr != nil {
					log.Error(serr, "Failed to update StorageVault status", "storageVault", storageVault.Name)
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
		log.Info("already has finalizer")
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

	// Check if our vault exists in the results
	exists := false
	for rows.Next() {
		var vaultName string
		var properties string
		if err := rows.Scan(&vaultName, &properties); err != nil {
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

	if !exists {
		log.Info("Storage vault does not exist")
	}
	return exists, nil
}

// createStorageVault creates a new storage vault in Doris
func (r *StorageVaultReconciler) createStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Creating new storage vault")

	// Build properties map
	properties := make(map[string]string)
	if storageVault.Spec.S3Properties != nil {
		s3Props := storageVault.Spec.S3Properties
		properties["s3.endpoint"] = s3Props.Endpoint
		properties["s3.region"] = s3Props.Region
		properties["s3.root.path"] = s3Props.RootPath
		properties["s3.bucket"] = s3Props.Bucket
		properties["provider"] = string(s3Props.Provider)
		if s3Props.UsePathStyle != nil {
			properties["use_path_style"] = strconv.FormatBool(*s3Props.UsePathStyle)
		}
	}

	// If S3 type, get credentials from secret
	if storageVault.Spec.Type == mysqlv1alpha1.S3Vault && storageVault.Spec.S3Properties != nil {
		// Get access key from secret if provided
		if storageVault.Spec.S3Properties.AccessKeySecretRef != nil {
			accessKeySecret := &v1.Secret{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: storageVault.Namespace,
				Name:      storageVault.Spec.S3Properties.AccessKeySecretRef.Name,
			}, accessKeySecret)
			if err != nil {
				log.Error(err, "Failed to get access key secret",
					"secretName", storageVault.Spec.S3Properties.AccessKeySecretRef.Name)
				return fmt.Errorf("failed to get access key secret: %w", err)
			}

			// Add access key to properties
			if accessKey, ok := accessKeySecret.Data[storageVault.Spec.S3Properties.AccessKeySecretRef.Key]; ok {
				properties["s3.access_key"] = string(accessKey)
				log.Info("Access key retrieved successfully")
			} else {
				log.Error(nil, "Access key not found in secret",
					"key", storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
				return fmt.Errorf("access key not found in secret using key %s",
					storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
			}
		}

		// Get secret key from secret if provided
		if storageVault.Spec.S3Properties.SecretKeySecretRef != nil {
			secretKeySecret := &v1.Secret{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: storageVault.Namespace,
				Name:      storageVault.Spec.S3Properties.SecretKeySecretRef.Name,
			}, secretKeySecret)
			if err != nil {
				log.Error(err, "Failed to get secret key secret",
					"secretName", storageVault.Spec.S3Properties.SecretKeySecretRef.Name)
				return fmt.Errorf("failed to get secret key secret: %w", err)
			}

			// Add secret key to properties
			if secretKey, ok := secretKeySecret.Data[storageVault.Spec.S3Properties.SecretKeySecretRef.Key]; ok {
				properties["s3.secret_key"] = string(secretKey)
				log.Info("Secret key retrieved successfully")
			} else {
				log.Error(nil, "Secret key not found in secret",
					"key", storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
				return fmt.Errorf("secret key not found in secret using key %s",
					storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
			}
		}
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	query := fmt.Sprintf("CREATE STORAGE VAULT IF NOT EXISTS '%s' PROPERTIES (%s)",
		storageVault.Spec.Name,
		strings.Join(props, ", "))

	log.Info("Executing create storage vault query")
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute CREATE STORAGE VAULT query")
		return err
	}

	log.Info("Storage vault created successfully")
	return nil
}

// updateStorageVault updates an existing storage vault in Doris
func (r *StorageVaultReconciler) updateStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler").WithValues("storageVault", storageVault.Spec.Name)
	log.Info("Updating storage vault")

	// Build properties map
	properties := make(map[string]string)
	if storageVault.Spec.S3Properties != nil {
		s3Props := storageVault.Spec.S3Properties
		properties["s3.endpoint"] = s3Props.Endpoint
		properties["s3.region"] = s3Props.Region
		properties["s3.root.path"] = s3Props.RootPath
		properties["s3.bucket"] = s3Props.Bucket
		properties["provider"] = string(s3Props.Provider)
		if s3Props.UsePathStyle != nil {
			properties["use_path_style"] = strconv.FormatBool(*s3Props.UsePathStyle)
		}
	}

	// If S3 type, get credentials from secret
	if storageVault.Spec.Type == mysqlv1alpha1.S3Vault && storageVault.Spec.S3Properties != nil {
		// Get access key from secret if provided
		if storageVault.Spec.S3Properties.AccessKeySecretRef != nil {
			accessKeySecret := &v1.Secret{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: storageVault.Namespace,
				Name:      storageVault.Spec.S3Properties.AccessKeySecretRef.Name,
			}, accessKeySecret)
			if err != nil {
				log.Error(err, "Failed to get access key secret",
					"secretName", storageVault.Spec.S3Properties.AccessKeySecretRef.Name)
				return fmt.Errorf("failed to get access key secret: %w", err)
			}

			// Add access key to properties
			if accessKey, ok := accessKeySecret.Data[storageVault.Spec.S3Properties.AccessKeySecretRef.Key]; ok {
				properties["s3.access_key"] = string(accessKey)
				log.Info("Access key retrieved successfully")
			} else {
				log.Error(nil, "Access key not found in secret",
					"key", storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
				return fmt.Errorf("access key not found in secret using key %s",
					storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
			}
		}

		// Get secret key from secret if provided
		if storageVault.Spec.S3Properties.SecretKeySecretRef != nil {
			secretKeySecret := &v1.Secret{}
			err := r.Get(ctx, types.NamespacedName{
				Namespace: storageVault.Namespace,
				Name:      storageVault.Spec.S3Properties.SecretKeySecretRef.Name,
			}, secretKeySecret)
			if err != nil {
				log.Error(err, "Failed to get secret key secret",
					"secretName", storageVault.Spec.S3Properties.SecretKeySecretRef.Name)
				return fmt.Errorf("failed to get secret key secret: %w", err)
			}

			// Add secret key to properties
			if secretKey, ok := secretKeySecret.Data[storageVault.Spec.S3Properties.SecretKeySecretRef.Key]; ok {
				properties["s3.secret_key"] = string(secretKey)
				log.Info("Secret key retrieved successfully")
			} else {
				log.Error(nil, "Secret key not found in secret",
					"key", storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
				return fmt.Errorf("secret key not found in secret using key %s",
					storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
			}
		}
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	query := fmt.Sprintf("ALTER STORAGE VAULT '%s' SET PROPERTIES (%s)",
		storageVault.Spec.Name,
		strings.Join(props, ", "))

	log.Info("Executing alter storage vault query")
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute ALTER STORAGE VAULT query")
		return err
	}

	log.Info("Storage vault updated successfully")
	return nil
}

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
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET DEFAULT STORAGE VAULT '%s'", storageVault.Spec.Name))
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
