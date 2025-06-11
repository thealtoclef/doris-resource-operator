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
	"maps"
	"regexp"
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
	"github.com/nakamasato/mysql-operator/internal/constants"
	metrics "github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	catalogFinalizer = "catalog.nakamasato.com/finalizer"
)

// CatalogReconciler reconciles a Catalog object
type CatalogReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
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
		catalog.Status.Phase = constants.PhaseNotReady
		catalog.Status.Reason = constants.ReasonMySQLFetchFailed
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
		catalog.Status.Phase = constants.PhaseNotReady
		catalog.Status.Reason = constants.ReasonMySQLConnectionFailed
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

	// Handle finalizer
	finalizerResult, finalizerErr := utils.HandleFinalizer(utils.FinalizerParams{
		Object:    catalog,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: catalogFinalizer,
		FinalizationFunc: func() error {
			return r.finalizeCatalog(ctx, mysqlClient, catalog)
		},
		OnFailure: func(err error) error {
			catalog.Status.Phase = constants.PhaseNotReady
			catalog.Status.Reason = constants.ReasonFailedToFinalize
			if serr := r.Status().Update(ctx, catalog); serr != nil {
				log.Error(serr, "Failed to update finalization status")
				return serr
			}
			return nil
		},
	})

	if finalizerErr != nil || !catalog.GetDeletionTimestamp().IsZero() {
		// If finalizer processing returned an error or object is being deleted,
		// return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. Catalog cannot be created.", "mysql", mysql.Name, "catalog", catalog.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Get the last known catalog name that was successfully created/updated in Doris
	catalogNameInDoris := r.getLastKnownCatalogName(catalog)
	log.Info("Catalog name tracking", "specName", catalog.Spec.Name, "lastKnownName", catalogNameInDoris)

	// Fetch catalog information from Doris - checks existence and gets properties in a single operation
	exists, createCatalogStmt, err := r.fetchCatalog(ctx, mysqlClient, catalogNameInDoris)
	if err != nil {
		log.Error(err, "Failed to fetch catalog information", "clusterName", clusterName, "catalogName", catalogNameInDoris)
		catalog.Status.Phase = constants.PhaseNotReady
		catalog.Status.Reason = constants.ReasonFailedToFetchCatalog
		if serr := r.Status().Update(ctx, catalog); serr != nil {
			log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Create catalog
		err := r.createCatalog(ctx, mysqlClient, catalog)
		if err != nil {
			log.Error(err, "Failed to create catalog")
			catalog.Status.Phase = constants.PhaseNotReady
			catalog.Status.Reason = constants.ReasonFailedToCreateCatalog
			if serr := r.Status().Update(ctx, catalog); serr != nil {
				log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}

		catalog.Status.CatalogCreated = true
		metrics.CatalogCreatedTotal.Increment()

		// Update last known name annotation after successful creation
		if err := r.updateLastKnownCatalogName(ctx, catalog); err != nil {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	} else {
		catalog.Status.CatalogCreated = true
		// Update catalog with the properties we fetched
		if err := r.updateCatalog(ctx, mysqlClient, catalog, catalogNameInDoris, createCatalogStmt); err != nil {
			log.Error(err, "Failed to update catalog")
			catalog.Status.Phase = constants.PhaseNotReady
			catalog.Status.Reason = constants.ReasonFailedToCreateCatalog
			if serr := r.Status().Update(ctx, catalog); serr != nil {
				log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}

		// Update last known name annotation after successful update
		if err := r.updateLastKnownCatalogName(ctx, catalog); err != nil {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	// Update status
	catalog.Status.Phase = constants.PhaseReady
	catalog.Status.Reason = constants.ReasonCompleted
	if serr := r.Status().Update(ctx, catalog); serr != nil {
		log.Error(serr, "Failed to update Catalog status", "catalog", catalog.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CatalogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Catalog{}).
		Complete(r)
}

// finalizeCatalog drops the catalog from Doris
func (r *CatalogReconciler) finalizeCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog) error {
	// Get the last known catalog name that was successfully created/updated in Doris
	catalogNameInDoris := r.getLastKnownCatalogName(catalog)
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalogNameInDoris)
	log.Info("Finalization requested")

	if !catalog.Status.CatalogCreated {
		log.Info("Catalog was not created, skipping finalization")
		return nil
	}

	// Check if we can drop the catalog - the internal catalog cannot be dropped
	if catalogNameInDoris == "internal" {
		log.Info("Cannot drop the internal catalog, skipping finalization")
		return nil
	}

	// Execute DROP CATALOG statement
	query := fmt.Sprintf("DROP CATALOG IF EXISTS `%s`", catalogNameInDoris)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute DROP CATALOG query")
		return err
	}

	log.Info("Catalog dropped successfully")
	return nil
}

// fetchCatalog checks if a catalog exists and returns its properties in a single operation
func (r *CatalogReconciler) fetchCatalog(ctx context.Context, db *sql.DB, name string) (bool, string, error) {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", name)
	log.Info("Fetching catalog information")

	// First check if the catalog exists
	query := fmt.Sprintf("SHOW CATALOGS LIKE '%s'", name)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SHOW CATALOGS LIKE query")
		return false, "", err
	}
	defer rows.Close()

	// Check if we have any rows to determine existence
	exists := false
	if rows.Next() {
		exists = true
	}

	if err := rows.Err(); err != nil {
		log.Error(err, "Error checking catalog existence")
		return false, "", err
	}

	// If catalog doesn't exist, return early
	if !exists {
		log.Info("Catalog does not exist")
		return false, "", nil
	}

	// Catalog exists, get its properties
	var catalogName string
	var createCatalogStmt string
	err = db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE CATALOG `%s`", name)).Scan(&catalogName, &createCatalogStmt)
	if err != nil {
		log.Error(err, "Failed to get catalog properties with SHOW CREATE CATALOG")
		return true, "", err // Catalog exists but we couldn't fetch properties
	}

	log.Info("Catalog exists and properties fetched")
	return true, createCatalogStmt, nil
}

// createCatalog creates a new catalog in Doris
func (r *CatalogReconciler) createCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog) error {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Spec.Name)
	log.Info("Creating new catalog")

	// Initialize properties map with values from the Properties field
	properties := make(map[string]string)
	maps.Copy(properties, catalog.Spec.Properties)

	// If PropertiesSecret is provided, get properties from the secret
	if catalog.Spec.PropertiesSecret != "" {
		propertiesSecret := &v1.Secret{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: catalog.Namespace,
			Name:      catalog.Spec.PropertiesSecret,
		}, propertiesSecret)
		if err != nil {
			log.Error(err, "Failed to get properties secret", "secretName", catalog.Spec.PropertiesSecret)
			return err
		}

		// Add all properties from the secret
		for k, v := range propertiesSecret.Data {
			properties[k] = string(v)
		}
	}

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
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

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute CREATE CATALOG query")
		return err
	}

	log.Info("Catalog created successfully")
	return nil
}

// updateCatalog updates an existing catalog in Doris
func (r *CatalogReconciler) updateCatalog(ctx context.Context, db *sql.DB, catalog *mysqlv1alpha1.Catalog, catalogNameInDoris string, currentCreateStmt string) error {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Spec.Name)
	log.Info("Checking if catalog needs updating")

	// Extract all information from the CREATE CATALOG statement
	currentProps := r.parsePropertiesFromCreateStmt(currentCreateStmt)
	currentComment := r.parseCommentFromCreateStmt(currentCreateStmt)

	// Initialize desired properties map with values from the Properties field
	desiredProps := make(map[string]string)
	for k, v := range catalog.Spec.Properties {
		if k != "type" { // type cannot be modified
			desiredProps[k] = v
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
			log.Error(err, "Failed to get properties secret", "secretName", catalog.Spec.PropertiesSecret)
			return err
		}

		// Add all properties from the secret
		for k, v := range propertiesSecret.Data {
			if k != "type" { // type cannot be modified
				desiredProps[k] = string(v)
			}
		}
	}

	// Compare properties to determine what needs to be updated
	propsToUpdate := make(map[string]string)
	for k, v := range desiredProps {
		if currentVal, exists := currentProps[k]; !exists || currentVal != v {
			propsToUpdate[k] = v
		}
	}

	// Update properties if there are any to update
	if len(propsToUpdate) > 0 {
		log.Info("Updating catalog properties", "count", len(propsToUpdate))

		// Build properties string
		props := make([]string, 0, len(propsToUpdate))
		for k, v := range propsToUpdate {
			props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
		}

		// Execute ALTER CATALOG query to update properties
		query := fmt.Sprintf("ALTER CATALOG `%s` SET PROPERTIES (%s)",
			catalogNameInDoris,
			strings.Join(props, ", "))

		_, err := db.ExecContext(ctx, query)
		if err != nil {
			log.Error(err, "Failed to execute ALTER CATALOG SET PROPERTIES query")
			return err
		}
		log.Info("Catalog properties updated successfully", "count", len(propsToUpdate))
	} else {
		log.Info("Catalog properties unchanged")
	}

	// Check if comment needs updating by comparing with current comment
	commentNeedsUpdate := catalog.Spec.Comment != "" && catalog.Spec.Comment != currentComment
	if commentNeedsUpdate {
		log.Info("Updating catalog comment")
		// Update comment
		commentQuery := fmt.Sprintf("ALTER CATALOG `%s` MODIFY COMMENT = '%s'",
			catalogNameInDoris, catalog.Spec.Comment)
		_, err := db.ExecContext(ctx, commentQuery)
		if err != nil {
			log.Error(err, "Failed to execute ALTER CATALOG MODIFY COMMENT query")
			return err
		}
		log.Info("Catalog comment updated successfully")
	} else if catalog.Spec.Comment != "" {
		log.Info("Catalog comment unchanged")
	}

	// Handle name change if needed
	if catalogNameInDoris != catalog.Spec.Name {
		log.Info("Updating catalog name")
		// Execute ALTER CATALOG RENAME statement
		renameQuery := fmt.Sprintf("ALTER CATALOG `%s` RENAME `%s`",
			catalogNameInDoris, catalog.Spec.Name)
		_, err := db.ExecContext(ctx, renameQuery)
		if err != nil {
			log.Error(err, "Failed to execute ALTER CATALOG RENAME query")
			return err
		}
		log.Info("Catalog renamed successfully")
	} else {
		log.Info("Catalog name unchanged")
	}

	return nil
}

// parsePropertiesFromCreateStmt extracts properties from CREATE CATALOG statement
func (r *CatalogReconciler) parsePropertiesFromCreateStmt(createStmt string) map[string]string {
	properties := make(map[string]string)

	// Extract properties section between PROPERTIES( and )
	propertiesIdx := strings.Index(createStmt, "PROPERTIES (")
	if propertiesIdx == -1 {
		return properties
	}

	propStart := propertiesIdx + len("PROPERTIES (")
	propEnd := strings.LastIndex(createStmt, ")")
	if propEnd <= propStart {
		return properties
	}

	propSection := createStmt[propStart:propEnd]

	// Use regex to match key-value pairs: "key" = "value" or 'key' = 'value'
	// This handles both double and single quotes for both keys and values
	keyValueRegex := regexp.MustCompile(`["']([^"']+)["']\s*=\s*["']([^"']*)["']`)
	matches := keyValueRegex.FindAllStringSubmatch(propSection, -1)

	for _, match := range matches {
		if len(match) == 3 {
			key := match[1]
			value := match[2]
			properties[key] = value
		}
	}

	return properties
}

// parseCommentFromCreateStmt extracts the comment from the CREATE CATALOG statement
func (r *CatalogReconciler) parseCommentFromCreateStmt(createStmt string) string {
	// Use regex to extract the comment part from CREATE CATALOG statement
	// This handles both single and double quoted comments
	commentRegex := regexp.MustCompile(`COMMENT\s+["']([^"']*)["']`)
	match := commentRegex.FindStringSubmatch(createStmt)

	if len(match) == 2 {
		return match[1]
	}

	return ""
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

// getLastKnownCatalogName returns the last known catalog name that was successfully created/updated
func (r *CatalogReconciler) getLastKnownCatalogName(catalog *mysqlv1alpha1.Catalog) string {
	if name, ok := catalog.Annotations[constants.CatalogLastKnownNameAnnotation]; ok && name != "" {
		return name
	}
	return catalog.Spec.Name
}

// updateLastKnownCatalogName sets the last known catalog name annotation and updates the resource
func (r *CatalogReconciler) updateLastKnownCatalogName(ctx context.Context, catalog *mysqlv1alpha1.Catalog) error {
	log := log.FromContext(ctx).WithName("CatalogReconciler").WithValues("catalog", catalog.Name)
	log.Info("Updating last known catalog name annotation")

	if catalog.Annotations == nil {
		catalog.Annotations = make(map[string]string)
	}
	catalog.Annotations[constants.CatalogLastKnownNameAnnotation] = catalog.Spec.Name

	// Save annotation changes
	if err := r.Update(ctx, catalog); err != nil {
		log.Error(err, "Failed to update Catalog annotations")
		return err
	}

	return nil
}
