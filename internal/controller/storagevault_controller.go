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

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
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

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StorageVaultReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")

	// Fetch StorageVault
	storageVault := &mysqlv1alpha1.StorageVault{}
	err := r.Get(ctx, req.NamespacedName, storageVault)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Info("[StorageVault] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		log.Error(err, "[StorageVault] Failed to fetch StorageVault")
		return ctrl.Result{}, err
	}
	log.Info("[StorageVault] Found", "name", storageVault.Name, "namespace", storageVault.Namespace)

	// Get MySQL cluster
	mysql := &mysqlv1alpha1.MySQL{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: req.Namespace,
		Name:      storageVault.Spec.ClusterName,
	}, mysql)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to fetch MySQL cluster", "clusterName", storageVault.Spec.ClusterName)
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonMySQLConnectionFailed
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "[StorageVault] Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[StorageVault] Found MySQL cluster", "clusterName", storageVault.Spec.ClusterName)

	// Finalize if DeletionTimestamp exists
	if !storageVault.GetDeletionTimestamp().IsZero() {
		log.Info("[StorageVault] Resource marked for deletion")
		if controllerutil.ContainsFinalizer(storageVault, storageVaultFinalizer) {
			// Run finalization logic for storageVaultFinalizer
			if err := r.finalizeStorageVault(ctx, storageVault); err != nil {
				log.Error(err, "[StorageVault] Failed to finalize StorageVault")
				storageVault.Status.Phase = storageVaultPhaseNotReady
				storageVault.Status.Reason = storageVaultReasonFailedToFinalize
				if serr := r.Status().Update(ctx, storageVault); serr != nil {
					log.Error(serr, "[StorageVault] Failed to update StorageVault status", "storageVault", storageVault.Name)
				}
				return ctrl.Result{}, err
			}
			log.Info("[StorageVault] Finalization completed")

			// Remove finalizer
			if controllerutil.RemoveFinalizer(storageVault, storageVaultFinalizer) {
				log.Info("[StorageVault] Removing finalizer")
				err := r.Update(ctx, storageVault)
				if err != nil {
					log.Error(err, "[StorageVault] Failed to remove finalizer")
					return ctrl.Result{}, err
				}
				log.Info("[StorageVault] Finalizer removed")
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil // Return success when not having the finalizer
	}

	// Add finalizer if not exists
	if controllerutil.AddFinalizer(storageVault, storageVaultFinalizer) {
		log.Info("[StorageVault] Added Finalizer")
		err = r.Update(ctx, storageVault)
		if err != nil {
			log.Error(err, "[StorageVault] Failed to update after adding finalizer")
			return ctrl.Result{}, err
		}
		log.Info("[StorageVault] Updated successfully after adding finalizer")
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("[StorageVault] MySQL is being deleted. StorageVault cannot be created.", "mysql", mysql.Name, "storageVault", storageVault.Name)
		return ctrl.Result{}, nil
	}

	// Reconcile storage vault
	result, err := r.reconcileStorageVault(ctx, storageVault)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to reconcile storage vault", "name", storageVault.Name)
		storageVault.Status.Phase = storageVaultPhaseNotReady
		storageVault.Status.Reason = storageVaultReasonFailedToCreateVault
		if serr := r.Status().Update(ctx, storageVault); serr != nil {
			log.Error(serr, "[StorageVault] Failed to update StorageVault status", "storageVault", storageVault.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return result, err
	}

	// Update status
	storageVault.Status.Phase = storageVaultPhaseReady
	storageVault.Status.Reason = storageVaultReasonCompleted
	if serr := r.Status().Update(ctx, storageVault); serr != nil {
		log.Error(serr, "[StorageVault] Failed to update StorageVault status", "storageVault", storageVault.Name)
	}
	log.Info("[StorageVault] Successfully reconciled", "name", storageVault.Name)

	return result, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StorageVaultReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.StorageVault{}).
		Complete(r)
}

// finalizeStorageVault drops the storage vault from Doris
func (r *StorageVaultReconciler) finalizeStorageVault(ctx context.Context, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")

	if !storageVault.Status.VaultCreated {
		log.Info("[StorageVault] Storage vault was not created, skipping finalization", "name", storageVault.Spec.Name)
		return nil
	}

	// Get MySQL cluster
	mysql := &mysqlv1alpha1.MySQL{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: storageVault.Namespace,
		Name:      storageVault.Spec.ClusterName,
	}, mysql)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Info("[StorageVault] MySQL cluster not found, cannot drop storage vault", "clusterName", storageVault.Spec.ClusterName)
			return nil
		}
		return fmt.Errorf("[StorageVault] failed to get MySQL cluster: %w", err)
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		log.Error(err, "[StorageVault] Failed to get MySQL client", "key", mysql.GetKey())
		return fmt.Errorf("[StorageVault] failed to get MySQL client: %w", err)
	}

	// Attempt to drop the storage vault
	log.Info("[StorageVault] Dropping storage vault", "name", storageVault.Spec.Name)
	_, err = mysqlClient.ExecContext(ctx, fmt.Sprintf("DROP STORAGE VAULT IF EXISTS '%s'", storageVault.Spec.Name))
	if err != nil {
		log.Error(err, "[StorageVault] Failed to drop storage vault", "name", storageVault.Spec.Name)
		return fmt.Errorf("[StorageVault] failed to drop storage vault: %w", err)
	}

	log.Info("[StorageVault] Successfully dropped storage vault", "name", storageVault.Spec.Name)
	return nil
}

// reconcileStorageVault reconciles the storage vault
func (r *StorageVaultReconciler) reconcileStorageVault(ctx context.Context, storageVault *mysqlv1alpha1.StorageVault) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")

	// Get the MySQL cluster
	mysql := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: storageVault.Namespace,
		Name:      storageVault.Spec.ClusterName,
	}, mysql); err != nil {
		log.Error(err, "[StorageVault] Failed to get MySQL cluster", "clusterName", storageVault.Spec.ClusterName)
		return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to get MySQL cluster: %w", err)
	}

	// Get MySQL client from MySQLClients
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		log.Error(err, "[StorageVault] Failed to get MySQL client", "key", mysql.GetKey(), "clusterName", storageVault.Spec.ClusterName)
		return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to get MySQL client: %w", err)
	}
	log.Info("[StorageVault] Successfully connected to MySQL client")

	// Check if storage vault exists
	exists, err := r.storageVaultExists(ctx, mysqlClient, storageVault.Spec.Name)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to check if storage vault exists", "name", storageVault.Spec.Name)
		return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to check if storage vault exists: %w", err)
	}

	if !exists {
		// Create storage vault if not exists
		if err := r.createStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "[StorageVault] Failed to create storage vault", "name", storageVault.Spec.Name)
			return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to create storage vault: %w", err)
		}
		log.Info("[StorageVault] Created storage vault successfully", "name", storageVault.Spec.Name)
		storageVault.Status.VaultCreated = true
	} else {
		storageVault.Status.VaultCreated = true
		// Update storage vault if exists
		if err := r.updateStorageVault(ctx, mysqlClient, storageVault); err != nil {
			log.Error(err, "[StorageVault] Failed to update storage vault", "name", storageVault.Spec.Name)
			return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to update storage vault: %w", err)
		}
		log.Info("[StorageVault] Updated storage vault successfully", "name", storageVault.Spec.Name)
	}

	// Handle default vault setting
	if err := r.handleDefaultVault(ctx, mysqlClient, storageVault); err != nil {
		log.Error(err, "[StorageVault] Failed to handle default storage vault", "name", storageVault.Spec.Name)
		return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to handle default storage vault: %w", err)
	}

	// Update status
	if err := r.Status().Update(ctx, storageVault); err != nil {
		log.Error(err, "[StorageVault] Failed to update storage vault status", "storageVault", storageVault.Name)
		return ctrl.Result{}, fmt.Errorf("[StorageVault] failed to update storage vault status: %w", err)
	}

	return ctrl.Result{}, nil
}

// storageVaultExists checks if a storage vault exists
func (r *StorageVaultReconciler) storageVaultExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")
	log.Info("[StorageVault] Checking if storage vault exists", "name", name)

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
		log.Info("[StorageVault] Storage vault does not exist", "name", name)
	}
	return exists, nil
}

// createStorageVault creates a new storage vault in Doris
func (r *StorageVaultReconciler) createStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")
	log.Info("[StorageVault] Creating new storage vault", "name", storageVault.Spec.Name)

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
				log.Error(err, "[StorageVault] Failed to get access key secret",
					"secretName", storageVault.Spec.S3Properties.AccessKeySecretRef.Name)
				return fmt.Errorf("[StorageVault] failed to get access key secret: %w", err)
			}

			// Add access key to properties
			if accessKey, ok := accessKeySecret.Data[storageVault.Spec.S3Properties.AccessKeySecretRef.Key]; ok {
				properties["s3.access_key"] = string(accessKey)
				log.Info("[StorageVault] Access key retrieved successfully")
			} else {
				log.Error(nil, "[StorageVault] Access key not found in secret",
					"key", storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
				return fmt.Errorf("[StorageVault] access key not found in secret using key %s",
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
				log.Error(err, "[StorageVault] Failed to get secret key secret",
					"secretName", storageVault.Spec.S3Properties.SecretKeySecretRef.Name)
				return fmt.Errorf("[StorageVault] failed to get secret key secret: %w", err)
			}

			// Add secret key to properties
			if secretKey, ok := secretKeySecret.Data[storageVault.Spec.S3Properties.SecretKeySecretRef.Key]; ok {
				properties["s3.secret_key"] = string(secretKey)
				log.Info("[StorageVault] Secret key retrieved successfully")
			} else {
				log.Error(nil, "[StorageVault] Secret key not found in secret",
					"key", storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
				return fmt.Errorf("[StorageVault] secret key not found in secret using key %s",
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

	log.Info("[StorageVault] Executing create storage vault query", "name", storageVault.Spec.Name)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to execute CREATE STORAGE VAULT query")
		return err
	}

	log.Info("[StorageVault] Storage vault created successfully", "name", storageVault.Spec.Name)
	return nil
}

// updateStorageVault updates an existing storage vault in Doris
func (r *StorageVaultReconciler) updateStorageVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")
	log.Info("[StorageVault] Updating storage vault", "name", storageVault.Spec.Name)

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
				log.Error(err, "[StorageVault] Failed to get access key secret",
					"secretName", storageVault.Spec.S3Properties.AccessKeySecretRef.Name)
				return fmt.Errorf("[StorageVault] failed to get access key secret: %w", err)
			}

			// Add access key to properties
			if accessKey, ok := accessKeySecret.Data[storageVault.Spec.S3Properties.AccessKeySecretRef.Key]; ok {
				properties["s3.access_key"] = string(accessKey)
				log.Info("[StorageVault] Access key retrieved successfully")
			} else {
				log.Error(nil, "[StorageVault] Access key not found in secret",
					"key", storageVault.Spec.S3Properties.AccessKeySecretRef.Key)
				return fmt.Errorf("[StorageVault] access key not found in secret using key %s",
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
				log.Error(err, "[StorageVault] Failed to get secret key secret",
					"secretName", storageVault.Spec.S3Properties.SecretKeySecretRef.Name)
				return fmt.Errorf("[StorageVault] failed to get secret key secret: %w", err)
			}

			// Add secret key to properties
			if secretKey, ok := secretKeySecret.Data[storageVault.Spec.S3Properties.SecretKeySecretRef.Key]; ok {
				properties["s3.secret_key"] = string(secretKey)
				log.Info("[StorageVault] Secret key retrieved successfully")
			} else {
				log.Error(nil, "[StorageVault] Secret key not found in secret",
					"key", storageVault.Spec.S3Properties.SecretKeySecretRef.Key)
				return fmt.Errorf("[StorageVault] secret key not found in secret using key %s",
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

	log.Info("[StorageVault] Executing alter storage vault query", "name", storageVault.Spec.Name)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "[StorageVault] Failed to execute ALTER STORAGE VAULT query")
		return err
	}

	log.Info("[StorageVault] Storage vault updated successfully", "name", storageVault.Spec.Name)
	return nil
}

func (r *StorageVaultReconciler) handleDefaultVault(ctx context.Context, db *sql.DB, storageVault *mysqlv1alpha1.StorageVault) error {
	log := log.FromContext(ctx).WithName("StorageVaultReconciler")
	// Get current default vault
	var currentDefault string
	err := db.QueryRowContext(ctx, "SHOW DEFAULT STORAGE VAULT").Scan(&currentDefault)
	if err != nil {
		return fmt.Errorf("[StorageVault] failed to get current default storage vault: %w", err)
	}

	log.Info("[StorageVault] Current default storage vault", "currentDefault", currentDefault)

	// Update status
	storageVault.Status.IsDefault = currentDefault == storageVault.Spec.Name

	// If this vault should be default and isn't currently default
	if storageVault.Spec.IsDefault != nil && *storageVault.Spec.IsDefault && currentDefault != storageVault.Spec.Name {
		log.Info("[StorageVault] Setting storage vault as default", "name", storageVault.Spec.Name)
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET DEFAULT STORAGE VAULT '%s'", storageVault.Spec.Name))
		if err != nil {
			return fmt.Errorf("[StorageVault] failed to set default storage vault: %w", err)
		}
		storageVault.Status.IsDefault = true
		log.Info("[StorageVault] Successfully set storage vault as default", "name", storageVault.Spec.Name)
	}

	return nil
}
