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
	catalogFinalizer                   = "catalog.nakamasato.com/finalizer"
	catalogReasonCompleted             = "Catalog successfully reconciled"
	catalogReasonMySQLConnectionFailed = "Failed to connect to cluster"
	catalogReasonFailedToCreateCatalog = "Failed to create catalog"
	catalogReasonFailedToGetSecret     = "Failed to get Secret"
	catalogReasonFailedToDropCatalog   = "Failed to drop catalog"
	catalogPhaseReady                  = "Ready"
	catalogPhaseNotReady               = "NotReady"
	catalogReasonFailedToFinalize      = "Failed to finalize catalog"
	catalogReasonMySQLFetchFailed      = "Failed to fetch cluster"
)

// CatalogReconciler reconciles a Catalog object
type CatalogReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
}

// NewCatalogReconciler creates a new CatalogReconciler
func NewCatalogReconciler(client client.Client, scheme *runtime.Scheme, mysqlClients mysqlinternal.MySQLClients) *CatalogReconciler {
	return &CatalogReconciler{
		Client:       client,
		Scheme:       scheme,
		MySQLClients: mysqlClients,
	}
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=catalogs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=catalogs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=catalogs/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *CatalogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("CatalogReconciler")

	// Fetch Catalog
	catalog := &mysqlv1alpha1.Catalog{}
	err := r.Get(ctx, req.NamespacedName, catalog)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchCatalog] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchCatalog] Failed")
		return ctrl.Result{}, err
	}
	log.Info("[FetchCatalog] Found", "name", catalog.Name, "namespace", catalog.Namespace)
	clusterName := catalog.Spec.ClusterName

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		catalog.Status.Phase = catalogPhaseNotReady
		catalog.Status.Reason = catalogReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, catalog); serr != nil {
			log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found")

	// SetOwnerReference if not exists
	if !r.ifOwnerReferencesContains(catalog.OwnerReferences, mysql) {
		err := controllerutil.SetControllerReference(mysql, catalog, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
		err = r.Update(ctx, catalog)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		catalog.Status.Phase = catalogPhaseNotReady
		catalog.Status.Reason = catalogReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, catalog); serr != nil {
			log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
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
	if !catalog.GetDeletionTimestamp().IsZero() {
		log.Info("Resource marked for deletion")
		if controllerutil.ContainsFinalizer(catalog, catalogFinalizer) {
			// Run finalization logic for catalogFinalizer
			if err := r.finalizeCatalog(ctx, mysqlClient, catalog); err != nil {
				log.Error(err, "Failed to finalize Catalog")
				catalog.Status.Phase = catalogPhaseNotReady
				catalog.Status.Reason = catalogReasonFailedToFinalize
				if serr := r.Status().Update(ctx, catalog); serr != nil {
					log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
				}
				return ctrl.Result{}, err
			}
			log.Info("Finalization completed")

			// Remove finalizer
			if controllerutil.RemoveFinalizer(catalog, catalogFinalizer) {
				log.Info("Removing finalizer")
				err := r.Update(ctx, catalog)
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
	if controllerutil.AddFinalizer(catalog, catalogFinalizer) {
		log.Info("Added Finalizer")
		err = r.Update(ctx, catalog)
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
		log.Info("MySQL is being deleted. Catalog cannot be created.", "mysql", mysql.Name, "catalog", catalog.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Check if catalog exists
	exists, catalogType, err := r.catalogExists(ctx, mysqlClient, catalog.Spec.Name)
	if err != nil {
		log.Error(err, "[Catalog] Failed to check if catalog exists", "clusterName", clusterName, "catalogName", catalog.Spec.Name)
		catalog.Status.Phase = catalogPhaseNotReady
		catalog.Status.Reason = catalogReasonFailedToCreateCatalog
		if serr := r.Status().Update(ctx, catalog); serr != nil {
			log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Create catalog if not exists
		catalogType, err = r.createCatalog(ctx, mysqlClient, catalog)
		if err != nil {
			log.Error(err, "Failed to create catalog")
			catalog.Status.Phase = catalogPhaseNotReady
			catalog.Status.Reason = catalogReasonFailedToCreateCatalog
			if serr := r.Status().Update(ctx, catalog); serr != nil {
				log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
		log.Info("Created catalog successfully")
		catalog.Status.CatalogCreated = true
		catalog.Status.Type = catalogType
		metrics.CatalogCreatedTotal.Increment()
	} else {
		catalog.Status.CatalogCreated = true
		catalog.Status.Type = catalogType
		// Update catalog if exists
		if err := r.updateCatalog(ctx, mysqlClient, catalog); err != nil {
			log.Error(err, "Failed to update catalog")
			catalog.Status.Phase = catalogPhaseNotReady
			catalog.Status.Reason = catalogReasonFailedToCreateCatalog
			if serr := r.Status().Update(ctx, catalog); serr != nil {
				log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
		log.Info("Updated catalog successfully")
	}

	// Update status
	catalog.Status.Phase = catalogPhaseReady
	catalog.Status.Reason = catalogReasonCompleted
	if serr := r.Status().Update(ctx, catalog); serr != nil {
		log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CatalogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Catalog{}).
		Complete(r)
}

// finalizeCatalog drops the catalog from Doris
func (r *CatalogReconciler) finalizeCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog) error {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Spec.Name)
	log.Info("Finalization requested")

	if !catalog.Status.CatalogCreated {
		log.Info("Catalog was not created, skipping finalization")
		return nil
	}

	// Check if we can drop the catalog - the internal catalog cannot be dropped
	if catalog.Spec.Name == "internal" {
		log.Info("Cannot drop the internal catalog, skipping finalization")
		return nil
	}

	// Execute DROP CATALOG statement
	query := fmt.Sprintf("DROP CATALOG IF EXISTS `%s`", catalog.Spec.Name)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to drop catalog")
		return fmt.Errorf("failed to drop catalog: %w", err)
	}

	log.Info("Catalog dropped successfully")
	return nil
}

// catalogExists checks if a catalog exists and returns its type
func (r *CatalogReconciler) catalogExists(ctx context.Context, db *sql.DB, name string) (bool, string, error) {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", name)
	log.Info("Checking if catalog exists")

	// Execute SHOW CATALOGS to get all catalogs
	rows, err := db.QueryContext(ctx, "SHOW CATALOGS")
	if err != nil {
		return false, "", err
	}
	defer rows.Close()

	// Check if our catalog exists in the results
	exists := false
	var catalogType string
	for rows.Next() {
		var catalogName string
		var catalogType string
		if err := rows.Scan(&catalogName, &catalogType); err != nil {
			return false, "", err
		}
		if catalogName == name {
			exists = true
			break
		}
	}

	if err := rows.Err(); err != nil {
		return false, "", err
	}

	// If catalog exists, get its type using SHOW CREATE CATALOG
	if exists {
		var createCatalogStmt string
		var catalogName string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE CATALOG `%s`", name)).Scan(&catalogName, &createCatalogStmt)
		if err != nil {
			return true, "", err
		}

		// Extract type from the CREATE CATALOG statement
		if typeMatch := strings.Contains(createCatalogStmt, `"type"`); typeMatch {
			// Simple parsing to extract the type value
			parts := strings.Split(createCatalogStmt, `"type"`)
			if len(parts) > 1 {
				valuePart := strings.Split(parts[1], `"`)
				if len(valuePart) > 2 {
					catalogType = valuePart[2]
				}
			}
		}
	} else {
		log.Info("Catalog does not exist")
	}

	return exists, catalogType, nil
}

// createCatalog creates a new catalog in Doris
func (r *CatalogReconciler) createCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog) (string, error) {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Spec.Name)
	log.Info("Creating new catalog")

	// Initialize properties map with values from the Properties field
	properties := make(map[string]string)
	for k, v := range catalog.Spec.Properties {
		properties[k] = v
	}

	// If PropertiesSecret is provided, get properties from the secret
	if catalog.Spec.PropertiesSecret != "" {
		propertiesSecret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: catalog.Namespace,
			Name:      catalog.Spec.PropertiesSecret,
		}, propertiesSecret)
		if err != nil {
			log.Error(err, "Failed to get properties secret",
				"secretName", catalog.Spec.PropertiesSecret)
			return "", fmt.Errorf("failed to get properties secret: %w", err)
		}

		// Add all properties from the secret
		for k, v := range propertiesSecret.Data {
			properties[k] = string(v)
			log.Info("Property retrieved from secret", "key", k)
		}
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	// Extract catalog type for status
	var catalogType string
	if t, ok := properties["type"]; ok {
		catalogType = t
	}

	// Build the CREATE CATALOG query
	var query string
	if catalog.Spec.Comment != "" {
		query = fmt.Sprintf("CREATE CATALOG IF NOT EXISTS `%s` COMMENT '%s' PROPERTIES (%s)",
			catalog.Spec.Name,
			catalog.Spec.Comment,
			strings.Join(props, ", "))
	} else {
		query = fmt.Sprintf("CREATE CATALOG IF NOT EXISTS `%s` PROPERTIES (%s)",
			catalog.Spec.Name,
			strings.Join(props, ", "))
	}

	log.Info("Executing create catalog query")
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute CREATE CATALOG query")
		return "", err
	}

	log.Info("Catalog created successfully")
	return catalogType, nil
}

// updateCatalog updates an existing catalog in Doris
func (r *CatalogReconciler) updateCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog) error {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Spec.Name)
	log.Info("Updating catalog")

	// Initialize properties map with values from the Properties field
	properties := make(map[string]string)
	for k, v := range catalog.Spec.Properties {
		if k != "type" { // type cannot be modified
			properties[k] = v
		}
	}

	// If PropertiesSecret is provided, get properties from the secret
	if catalog.Spec.PropertiesSecret != "" {
		propertiesSecret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: catalog.Namespace,
			Name:      catalog.Spec.PropertiesSecret,
		}, propertiesSecret)
		if err != nil {
			log.Error(err, "Failed to get properties secret",
				"secretName", catalog.Spec.PropertiesSecret)
			return fmt.Errorf("failed to get properties secret: %w", err)
		}

		// Add all properties from the secret
		for k, v := range propertiesSecret.Data {
			if k != "type" { // type cannot be modified
				properties[k] = string(v)
				log.Info("Property retrieved from secret", "key", k)
			}
		}
	}

	// Skip update if no properties to update
	if len(properties) == 0 {
		log.Info("No properties to update")
		return nil
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	// Execute ALTER CATALOG query to update properties
	query := fmt.Sprintf("ALTER CATALOG `%s` SET PROPERTIES (%s)",
		catalog.Spec.Name,
		strings.Join(props, ", "))

	log.Info("Executing alter catalog query")
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute ALTER CATALOG query")
		return err
	}

	// Update comment if provided and different from current
	if catalog.Spec.Comment != "" {
		commentQuery := fmt.Sprintf("ALTER CATALOG `%s` MODIFY COMMENT '%s'",
			catalog.Spec.Name,
			catalog.Spec.Comment)

		log.Info("Executing alter catalog comment query")
		_, err := db.ExecContext(ctx, commentQuery)
		if err != nil {
			log.Error(err, "Failed to execute ALTER CATALOG comment query")
			return err
		}
	}

	log.Info("Catalog updated successfully")
	return nil
}

// ifOwnerReferencesContains checks if the ownerReferences contains the MySQL object
func (r *CatalogReconciler) ifOwnerReferencesContains(ownerReferences []metav1.OwnerReference, mysql *mysqlv1alpha1.MySQL) bool {
	for _, ref := range ownerReferences {
		if ref.APIVersion == "mysql.nakamasato.com/v1alpha1" && ref.Kind == "MySQL" && ref.UID == mysql.UID {
			return true
		}
	}
	return false
}
