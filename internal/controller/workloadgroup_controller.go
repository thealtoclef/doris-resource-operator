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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	_ "github.com/go-sql-driver/mysql"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"maps"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	"github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	workloadGroupFinalizer = "workloadgroup.nakamasato.com/finalizer"
)

// WorkloadGroupReconciler reconciles a WorkloadGroup object
type WorkloadGroupReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=workloadgroups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=workloadgroups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=workloadgroups/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *WorkloadGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("WorkloadGroupReconciler")

	// Fetch WorkloadGroup
	workloadGroup := &mysqlv1alpha1.WorkloadGroup{}
	err := r.Get(ctx, req.NamespacedName, workloadGroup)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchWorkloadGroup] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchWorkloadGroup] Failed")
		return ctrl.Result{}, err
	}
	log.Info("[FetchWorkloadGroup] Found", "name", workloadGroup.Name, "namespace", workloadGroup.Namespace, "workloadGroupName", workloadGroup.Spec.Name)
	clusterName := workloadGroup.Spec.ClusterName

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed", "clusterName", clusterName)
		workloadGroup.Status.Phase = constants.PhaseNotReady
		workloadGroup.Status.Reason = constants.ReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
			log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found", "mysql", mysql.Name)

	// SetOwnerReference if not exists
	if !r.ifOwnerReferencesContains(workloadGroup.OwnerReferences, mysql) {
		err := controllerutil.SetControllerReference(mysql, workloadGroup, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
		err = r.Update(ctx, workloadGroup)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		workloadGroup.Status.Phase = constants.PhaseNotReady
		workloadGroup.Status.Reason = constants.ReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
			log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
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
		Object:    workloadGroup,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: workloadGroupFinalizer,
		FinalizationFunc: func() error {
			return r.finalizeWorkloadGroup(ctx, mysqlClient, workloadGroup)
		},
		OnFailure: func(err error) error {
			workloadGroup.Status.Phase = constants.PhaseNotReady
			workloadGroup.Status.Reason = constants.ReasonFailedToFinalize
			if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
				log.Error(serr, "Failed to update finalization status")
				return serr
			}
			return nil
		},
	})

	if finalizerErr != nil || !workloadGroup.GetDeletionTimestamp().IsZero() {
		// If finalizer processing returned an error or object is being deleted,
		// return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. WorkloadGroup cannot be created.", "mysql", mysql.Name, "workloadGroup", workloadGroup.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Fetch workload group information from Doris - checks existence and gets properties in a single operation
	exists, properties, err := r.fetchWorkloadGroup(ctx, mysqlClient, workloadGroup.Spec.Name)
	if err != nil {
		log.Error(err, "Failed to fetch workload group information", "clusterName", clusterName, "workloadGroupName", workloadGroup.Spec.Name)
		workloadGroup.Status.Phase = constants.PhaseNotReady
		workloadGroup.Status.Reason = constants.ReasonFailedToFetchWorkloadGroup
		if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
			log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Create workload group
		err := r.createWorkloadGroup(ctx, mysqlClient, workloadGroup)
		if err != nil {
			log.Error(err, "Failed to create workload group", "name", workloadGroup.Spec.Name)
			workloadGroup.Status.Phase = constants.PhaseNotReady
			workloadGroup.Status.Reason = constants.ReasonFailedToCreateWorkloadGroup
			if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
				log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}

		workloadGroup.Status.WorkloadGroupCreated = true
		metrics.WorkloadGroupCreatedTotal.Increment()
	} else {
		workloadGroup.Status.WorkloadGroupCreated = true
		// Update workload group with the fetched properties
		if err := r.updateWorkloadGroup(ctx, mysqlClient, workloadGroup, properties); err != nil {
			log.Error(err, "Failed to update workload group", "name", workloadGroup.Spec.Name)
			workloadGroup.Status.Phase = constants.PhaseNotReady
			workloadGroup.Status.Reason = constants.ReasonFailedToUpdateWorkloadGroup
			if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
				log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err // requeue
		}
	}

	// Update status
	workloadGroup.Status.Phase = constants.PhaseReady
	workloadGroup.Status.Reason = constants.ReasonCompleted
	if serr := r.Status().Update(ctx, workloadGroup); serr != nil {
		log.Error(serr, "Failed to update WorkloadGroup status", "workloadGroup", workloadGroup.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.WorkloadGroup{}).
		Complete(r)
}

// finalizeWorkloadGroup drops the workload group from Doris
func (r *WorkloadGroupReconciler) finalizeWorkloadGroup(ctx context.Context, db *sql.DB, workloadGroup *mysqlv1alpha1.WorkloadGroup) error {
	log := log.FromContext(ctx).WithName("WorkloadGroupReconciler").WithValues("workloadGroup", workloadGroup.Spec.Name)
	log.Info("Finalization requested")

	if !workloadGroup.Status.WorkloadGroupCreated {
		log.Info("WorkloadGroup was not created, skipping finalization")
		return nil
	}

	// Execute DROP WORKLOAD GROUP statement
	query := fmt.Sprintf("DROP WORKLOAD GROUP IF EXISTS %s", workloadGroup.Spec.Name)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute DROP WORKLOAD GROUP query")
		return err
	}

	log.Info("WorkloadGroup dropped successfully")
	metrics.WorkloadGroupDeletedTotal.Increment()
	return nil
}

// fetchWorkloadGroup checks if a workload group exists and returns its properties in a single operation
func (r *WorkloadGroupReconciler) fetchWorkloadGroup(ctx context.Context, db *sql.DB, name string) (bool, map[string]string, error) {
	log := log.FromContext(ctx).WithName("WorkloadGroupReconciler").WithValues("workloadGroup", name)
	log.Info("Fetching workload group information")

	// Execute SHOW WORKLOAD GROUPS LIKE query
	query := fmt.Sprintf("SHOW WORKLOAD GROUPS LIKE '%s'", name)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SHOW WORKLOAD GROUPS LIKE query")
		return false, nil, err
	}
	defer rows.Close()

	// Get column names from the result set
	columns, err := rows.Columns()
	if err != nil {
		log.Error(err, "Error getting column names from result")
		return false, nil, err
	}

	// Check if we have any rows to determine existence
	exists := false
	properties := make(map[string]string)

	// Create values slice to hold the column values
	values := make([]any, len(columns))
	// Create a slice of pointers to the values
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Process rows
	if rows.Next() {
		exists = true
		// Scan the row into the slice of pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Error(err, "Error scanning row from SHOW WORKLOAD GROUPS result")
			return true, nil, err // Workload group exists but we failed to scan properties
		}

		// Convert the values to strings and populate the properties map
		// Skip 'Id' and 'Name' columns as they're not properties
		for i, col := range columns {
			if col != "Id" && col != "Name" {
				// Convert the value to string based on its type
				var strVal string
				if values[i] == nil {
					strVal = ""
				} else {
					// Handle different types of values
					switch v := values[i].(type) {
					case []byte:
						strVal = string(v)
					default:
						strVal = fmt.Sprintf("%v", v)
					}
				}
				properties[col] = strVal
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Error(err, "Error iterating over workload group properties")
		return false, nil, err
	}

	if exists {
		log.Info("Workload group exists and properties fetched")
		return true, properties, nil
	}

	log.Info("Workload group does not exist")
	return false, nil, nil
}

// createWorkloadGroup creates a new workload group in Doris
func (r *WorkloadGroupReconciler) createWorkloadGroup(ctx context.Context, db *sql.DB, workloadGroup *mysqlv1alpha1.WorkloadGroup) error {
	log := log.FromContext(ctx).WithName("WorkloadGroupReconciler").WithValues("workloadGroup", workloadGroup.Spec.Name)
	log.Info("Creating new workload group")

	// Initialize properties map with values from the Properties field
	properties := make(map[string]string)
	maps.Copy(properties, workloadGroup.Spec.Properties)

	// Build properties string
	props := make([]string, 0, len(properties))
	for k, v := range properties {
		props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
	}

	// Build the CREATE WORKLOAD GROUP query
	query := fmt.Sprintf("CREATE WORKLOAD GROUP IF NOT EXISTS %s PROPERTIES (%s)",
		workloadGroup.Spec.Name,
		strings.Join(props, ", "))

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute CREATE WORKLOAD GROUP query")
		return err
	}

	log.Info("WorkloadGroup created successfully")
	return nil
}

// updateWorkloadGroup updates an existing workload group in Doris
func (r *WorkloadGroupReconciler) updateWorkloadGroup(ctx context.Context, db *sql.DB, workloadGroup *mysqlv1alpha1.WorkloadGroup, currentProps map[string]string) error {
	log := log.FromContext(ctx).WithName("WorkloadGroupReconciler").WithValues("workloadGroup", workloadGroup.Spec.Name)
	log.Info("Checking if workload group needs updating")

	// Initialize desired properties map with values from the Properties field
	desiredProps := make(map[string]string)
	maps.Copy(desiredProps, workloadGroup.Spec.Properties)

	// Check if properties need updating by comparing current vs desired
	propsToUpdate := make(map[string]string)
	for k, v := range desiredProps {
		// If the property doesn't exist or has changed
		if currentValue, exists := currentProps[k]; !exists || currentValue != v {
			propsToUpdate[k] = v
		}
	}

	// Update properties if there are any to update
	if len(propsToUpdate) > 0 {
		log.Info("Updating workload group properties", "count", len(propsToUpdate))
		// Build properties string for the properties that need updating
		props := make([]string, 0, len(propsToUpdate))
		for k, v := range propsToUpdate {
			props = append(props, fmt.Sprintf("'%s'='%s'", k, v))
		}

		// Execute ALTER WORKLOAD GROUP query to update properties
		query := fmt.Sprintf("ALTER WORKLOAD GROUP %s PROPERTIES (%s)",
			workloadGroup.Spec.Name,
			strings.Join(props, ", "))

		_, err := db.ExecContext(ctx, query)
		if err != nil {
			log.Error(err, "Failed to execute ALTER WORKLOAD GROUP query")
			return err
		}
		log.Info("WorkloadGroup properties updated successfully", "count", len(propsToUpdate))
	} else {
		log.Info("WorkloadGroup properties unchanged")
	}

	return nil
}

// ifOwnerReferencesContains checks if the ownerReferences contains the MySQL object
func (r *WorkloadGroupReconciler) ifOwnerReferencesContains(ownerReferences []metav1.OwnerReference, mysql *mysqlv1alpha1.MySQL) bool {
	for _, ref := range ownerReferences {
		if ref.APIVersion == "mysql.nakamasato.com/v1alpha1" && ref.Kind == "MySQL" && ref.UID == mysql.UID {
			return true
		}
	}
	return false
}
