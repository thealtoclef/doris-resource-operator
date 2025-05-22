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

	"gopkg.in/yaml.v3"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	globalVariableFinalizer = "globalvariable.nakamasato.com/finalizer"
)

// GlobalVariableReconciler reconciles a GlobalVariable object
type GlobalVariableReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=globalvariables,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=globalvariables/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=globalvariables/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *GlobalVariableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("GlobalVariableReconciler")

	// Fetch GlobalVariable
	globalVariable := &mysqlv1alpha1.GlobalVariable{}
	err := r.Get(ctx, req.NamespacedName, globalVariable)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchGlobalVariable] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchGlobalVariable] Failed")
		return ctrl.Result{}, err
	}
	log.Info("[FetchGlobalVariable] Found", "name", globalVariable.Name, "namespace", globalVariable.Namespace)
	clusterName := globalVariable.Spec.ClusterName

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		globalVariable.Status.Phase = constants.PhaseNotReady
		globalVariable.Status.Reason = constants.ReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, globalVariable); serr != nil {
			log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found")

	// SetOwnerReference if not exists
	if !r.ifOwnerReferencesContains(globalVariable.OwnerReferences, mysql) {
		err := controllerutil.SetControllerReference(mysql, globalVariable, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
		err = r.Update(ctx, globalVariable)
		if err != nil {
			return ctrl.Result{}, err // requeue
		}
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		globalVariable.Status.Phase = constants.PhaseNotReady
		globalVariable.Status.Reason = constants.ReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, globalVariable); serr != nil {
			log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
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
		Object:    globalVariable,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: globalVariableFinalizer,
		FinalizationFunc: func() error {
			return r.finalizeGlobalVariable(ctx, mysqlClient, globalVariable)
		},
		OnFailure: func(err error) error {
			globalVariable.Status.Phase = constants.PhaseNotReady
			globalVariable.Status.Reason = constants.ReasonFailedToFinalize
			if serr := r.Status().Update(ctx, globalVariable); serr != nil {
				log.Error(serr, "Failed to update finalization status")
				return serr
			}
			return nil
		},
	})

	if finalizerErr != nil || !globalVariable.GetDeletionTimestamp().IsZero() {
		// If finalizer processing returned an error or object is being deleted,
		// return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// Skip if MySQL is being deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. GlobalVariable cannot be created.", "mysql", mysql.Name, "globalVariable", globalVariable.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Fetch variable information from Doris
	exists, currentValue, err := r.fetchGlobalVariable(ctx, mysqlClient, globalVariable.Spec.Name)
	if err != nil {
		log.Error(err, "Failed to fetch global variable information", "clusterName", clusterName, "variableName", globalVariable.Spec.Name)
		globalVariable.Status.Phase = constants.PhaseNotReady
		globalVariable.Status.Reason = constants.ReasonFailedToFetchVariable
		if serr := r.Status().Update(ctx, globalVariable); serr != nil {
			log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	if !exists {
		// Variable doesn't exist, set status to NotReady
		log.Info("Global variable does not exist", "variableName", globalVariable.Spec.Name)
		globalVariable.Status.Phase = constants.PhaseNotReady
		globalVariable.Status.Reason = constants.ReasonVariableNotExist
		globalVariable.Status.VariableCreated = false

		if serr := r.Status().Update(ctx, globalVariable); serr != nil {
			log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
	}

	// Variable exists, update if needed
	if err := r.updateGlobalVariable(ctx, mysqlClient, globalVariable, currentValue); err != nil {
		log.Error(err, "Failed to update global variable")
		globalVariable.Status.Phase = constants.PhaseNotReady
		globalVariable.Status.Reason = constants.ReasonFailedToUpdateVariable
		if serr := r.Status().Update(ctx, globalVariable); serr != nil {
			log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err // requeue
	}

	// Update status
	globalVariable.Status.Phase = constants.PhaseReady
	globalVariable.Status.Reason = constants.ReasonCompleted
	globalVariable.Status.VariableCreated = true
	if serr := r.Status().Update(ctx, globalVariable); serr != nil {
		log.Error(serr, "Failed to update GlobalVariable status", "globalVariable", globalVariable.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GlobalVariableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.GlobalVariable{}).
		Complete(r)
}

// finalizeGlobalVariable unsets the global variable in Doris
func (r *GlobalVariableReconciler) finalizeGlobalVariable(ctx context.Context, db *sql.DB, globalVariable *mysqlv1alpha1.GlobalVariable) error {
	log := log.FromContext(ctx).WithName("GlobalVariableReconciler").WithValues("globalVariable", globalVariable.Spec.Name)
	log.Info("Finalization requested")

	if !globalVariable.Status.VariableCreated {
		log.Info("Global variable was not created, skipping finalization")
		return nil
	}

	// Execute UNSET GLOBAL VARIABLE statement
	query := fmt.Sprintf("UNSET GLOBAL VARIABLE %s", globalVariable.Spec.Name)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute UNSET GLOBAL VARIABLE query")
		return err
	}

	log.Info("Global variable unset successfully")
	return nil
}

// fetchGlobalVariable checks if a global variable exists and returns its current value
func (r *GlobalVariableReconciler) fetchGlobalVariable(ctx context.Context, db *sql.DB, name string) (bool, string, error) {
	log := log.FromContext(ctx).WithName("GlobalVariableReconciler").WithValues("variable", name)
	log.Info("Fetching global variable information")

	// Execute SHOW VARIABLES to get the variable value
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", name)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SHOW VARIABLES query")
		return false, "", err
	}
	defer rows.Close()

	// Check if we have any rows to determine existence
	exists := false
	var value string
	if rows.Next() {
		exists = true
		var varName, defaultValue, changed string
		if err := rows.Scan(&varName, &value, &defaultValue, &changed); err != nil {
			log.Error(err, "Failed to scan variable row")
			return false, "", err
		}
	}

	if err := rows.Err(); err != nil {
		log.Error(err, "Error checking variable existence")
		return false, "", err
	}

	if !exists {
		log.Info("Global variable does not exist")
		return false, "", nil
	}

	log.Info("Global variable exists and value fetched")
	return true, value, nil
}

// compareVariableValues compares two variable values
// Returns true if the values are equal, false otherwise
func (r *GlobalVariableReconciler) compareVariableValues(currentValue, desiredValue string) bool {
	// Normalize values by trimming whitespace
	currentValue = strings.TrimSpace(currentValue)
	desiredValue = strings.TrimSpace(desiredValue)

	// Simple string comparison - let SQL handle type conversion
	return currentValue == desiredValue
}

// formatValueForSQL formats the value for SQL using YAML parsing for type inference
func (r *GlobalVariableReconciler) formatValueForSQL(value string) string {
	// Trim whitespace
	value = strings.TrimSpace(value)

	// Parse the value using YAML to infer its type
	var parsedValue any
	if err := yaml.Unmarshal([]byte(value), &parsedValue); err != nil {
		// If YAML parsing fails, treat it as a string
		return fmt.Sprintf("'%s'", value)
	}

	// Format based on the inferred type
	switch v := parsedValue.(type) {
	case bool:
		return fmt.Sprintf("%t", v)
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case string:
		return fmt.Sprintf("'%s'", v)
	default:
		// For any other type, convert to string and wrap in quotes
		return fmt.Sprintf("'%s'", value)
	}
}

// updateGlobalVariable updates an existing global variable in Doris
func (r *GlobalVariableReconciler) updateGlobalVariable(ctx context.Context, db *sql.DB, globalVariable *mysqlv1alpha1.GlobalVariable, currentValue string) error {
	log := log.FromContext(ctx).WithName("GlobalVariableReconciler").WithValues("variable", globalVariable.Spec.Name)
	log.Info("Checking if global variable needs updating")

	// Compare current and desired values
	if r.compareVariableValues(currentValue, globalVariable.Spec.Value) {
		log.Info("Global variable value unchanged")
		return nil
	}

	// Format the value for SQL
	formattedValue := r.formatValueForSQL(globalVariable.Spec.Value)

	// Build and execute the SET GLOBAL query
	query := fmt.Sprintf("SET GLOBAL %s = %s", globalVariable.Spec.Name, formattedValue)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SET GLOBAL query")
		return err
	}

	log.Info("Global variable updated successfully")
	return nil
}

// ifOwnerReferencesContains checks if the ownerReferences contains the MySQL object
func (r *GlobalVariableReconciler) ifOwnerReferencesContains(ownerReferences []metav1.OwnerReference, mysql *mysqlv1alpha1.MySQL) bool {
	for _, ref := range ownerReferences {
		if ref.APIVersion == "mysql.nakamasato.com/v1alpha1" && ref.Kind == "MySQL" && ref.UID == mysql.UID {
			return true
		}
	}
	return false
}
