/*
Copyright 2023.

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
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	_ "github.com/go-sql-driver/mysql"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	"github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	mysqlUserFinalizer = "mysqluser.nakamasato.com/finalizer"
)

// PropertyDefaultValues defines the fallback sequence for setting default property values
// When a property is not defined, we try these values in order: ” -> '-1' -> 'normal' -> '100'
var PropertyDefaultValues = []string{"", "-1", "normal", "100"}

// MySQLUserReconciler reconciles a MySQLUser object
type MySQLUserReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	MySQLClients mysqlinternal.MySQLClients
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqlusers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqlusers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqlusers/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch

// Reconcile function is responsible for managing MySQLUser.
// Create MySQL user if not exist in the target MySQL, and drop it
// if the corresponding object is deleted.
func (r *MySQLUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler")

	// Fetch MySQLUser
	mysqlUser := &mysqlv1alpha1.MySQLUser{}
	err := r.Get(ctx, req.NamespacedName, mysqlUser)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchMySQLUser] Not found", "req.NamespacedName", req.NamespacedName)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchMySQLUser] Failed")
		return ctrl.Result{}, err
	}
	log.Info("[FetchMySQLUser] Found.", "name", mysqlUser.Name, "mysqlUser.Namespace", mysqlUser.Namespace)
	clusterName := mysqlUser.Spec.ClusterName
	userIdentity := mysqlUser.GetUserIdentity()
	passwordSecretRef := mysqlUser.Spec.PasswordSecretRef
	grants := mysqlUser.Spec.Grants
	properties := mysqlUser.Spec.Properties

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		mysqlUser.Status.Phase = constants.PhaseNotReady
		mysqlUser.Status.Reason = constants.ReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[FetchMySQL] Found")

	// SetOwnerReference if not exists
	if !r.ifOwnerReferencesContains(mysqlUser.OwnerReferences, mysql) {
		err := controllerutil.SetControllerReference(mysql, mysqlUser, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err //requeue
		}
		err = r.Update(ctx, mysqlUser)
		if err != nil {
			return ctrl.Result{}, err //requeue
		}
	}

	// Get MySQL client
	mysqlClient, err := r.MySQLClients.GetClient(mysql.GetKey())
	if err != nil {
		mysqlUser.Status.Phase = constants.PhaseNotReady
		mysqlUser.Status.Reason = constants.ReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
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
		Object:    mysqlUser,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: mysqlUserFinalizer,
		FinalizationFunc: func() error {
			return r.finalizeMySQLUser(ctx, mysqlClient, mysqlUser)
		},
		OnFailure: func(err error) error {
			mysqlUser.Status.Phase = constants.PhaseNotReady
			mysqlUser.Status.Reason = constants.ReasonFailedToFinalize
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update finalization status")
				return serr
			}
			return nil
		},
	})

	if finalizerErr != nil || !mysqlUser.GetDeletionTimestamp().IsZero() {
		// If finalizer processing returned an error or object is being deleted,
		// return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// Skip all the following steps if MySQL is being Deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. MySQLUser cannot be created.", "mysql", mysql.Name, "mysqlUser", mysqlUser.Name)
		return ctrl.Result{}, nil // Return success but skip further reconciliation
	}

	// Get password from Secret
	secret := &v1.Secret{}
	err = r.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: passwordSecretRef.Name}, secret)
	if err != nil {
		log.Error(err, "[password] Failed to get Secret", "passwordSecretRef", passwordSecretRef)
		mysqlUser.Status.Phase = constants.PhaseNotReady
		mysqlUser.Status.Reason = constants.ReasonFailedToGetSecret
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.Info("[password] Get password from Secret", "passwordSecretRef", passwordSecretRef)
	password := string(secret.Data[passwordSecretRef.Key])

	// Check if MySQL user exists
	_, err = mysqlClient.ExecContext(ctx, fmt.Sprintf("SHOW GRANTS FOR %s", userIdentity))
	if err != nil {
		// Create User if not exists with the password set above.
		_, err = mysqlClient.ExecContext(ctx,
			fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'", userIdentity, password))
		if err != nil {
			log.Error(err, "[MySQL] Failed to create User", "clusterName", clusterName, "userIdentity", userIdentity)
			mysqlUser.Status.Phase = constants.PhaseNotReady
			mysqlUser.Status.Reason = constants.ReasonMySQLFailedToCreateUser
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
			}
			return ctrl.Result{}, err //requeue
		}
		log.Info("[MySQL] Created User", "clusterName", clusterName, "userIdentity", userIdentity)
		mysqlUser.Status.UserCreated = true
		metrics.MysqlUserCreatedTotal.Increment()
	} else {
		mysqlUser.Status.UserCreated = true
		// Update password of User if already exists with the password set above.
		_, err = mysqlClient.ExecContext(ctx,
			fmt.Sprintf("ALTER USER %s IDENTIFIED BY '%s'", userIdentity, password))
		if err != nil {
			log.Error(err, "[MySQL] Failed to update password of User", "clusterName", clusterName, "userIdentity", userIdentity)
			mysqlUser.Status.Phase = constants.PhaseNotReady
			mysqlUser.Status.Reason = constants.ReasonMySQLFailedToUpdatePassword
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
			}
			return ctrl.Result{}, err //requeue
		}
		log.Info("[MySQL] Updated password of User", "clusterName", clusterName, "userIdentity", userIdentity)
	}

	// Update Grants
	if mysqlUser.Spec.ManageGrants {
		err = r.updateGrants(ctx, mysqlClient, userIdentity, grants)
		if err != nil {
			log.Error(err, "[MySQL] Failed to update Grants", "clusterName", clusterName, "userIdentity", userIdentity)
			mysqlUser.Status.Phase = constants.PhaseNotReady
			mysqlUser.Status.Reason = constants.ReasonMySQLFailedToGrant
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
			}
			return ctrl.Result{}, err
		}
	}

	// Update Properties
	if mysqlUser.Spec.ManageProperties {
		err = r.updateProperties(ctx, mysqlClient, mysqlUser.Spec.Username, properties)
		if err != nil {
			log.Error(err, "[MySQL] Failed to update Properties", "clusterName", clusterName, "username", mysqlUser.Spec.Username)
			mysqlUser.Status.Phase = constants.PhaseNotReady
			mysqlUser.Status.Reason = constants.ReasonMySQLFailedToSetProperty
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
			}
			return ctrl.Result{}, err
		}
	}

	// Update status
	mysqlUser.Status.Phase = constants.PhaseReady
	mysqlUser.Status.Reason = constants.ReasonCompleted
	if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
		log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MySQLUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.MySQLUser{}).
		Complete(r)
}

// finalizeMySQLUser drops MySQL user
func (r *MySQLUserReconciler) finalizeMySQLUser(ctx context.Context, mysqlClient *sql.DB, mysqlUser *mysqlv1alpha1.MySQLUser) error {
	if mysqlUser.Status.UserCreated {
		// Only revoke grants if ManageGrants is true
		if mysqlUser.Spec.ManageGrants {
			// Get existing grants
			existingGrants, err := r.fetchGrants(ctx, mysqlClient, mysqlUser.GetUserIdentity())
			if err != nil {
				return err
			}
			// Revoke all existing grants
			if err := r.revokePrivileges(ctx, mysqlClient, mysqlUser.GetUserIdentity(), existingGrants); err != nil {
				return err
			}
		}
		// Drop the user
		_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%s'", mysqlUser.Spec.Username, mysqlUser.Spec.Host))
		if err != nil {
			return err
		}
		metrics.MysqlUserDeletedTotal.Increment()
	}

	return nil
}

func (r *MySQLUserReconciler) ifOwnerReferencesContains(ownerReferences []metav1.OwnerReference, mysql *mysqlv1alpha1.MySQL) bool {
	for _, ref := range ownerReferences {
		if ref.APIVersion == "mysql.nakamasato.com/v1alpha1" && ref.Kind == "MySQL" && ref.UID == mysql.UID {
			return true
		}
	}
	return false
}

// PrivilegeType represents the type of privilege in Doris
type PrivilegeType string

const (
	// Global privileges
	NodePriv  PrivilegeType = "NODE_PRIV"
	AdminPriv PrivilegeType = "ADMIN_PRIV"
	GrantPriv PrivilegeType = "GRANT_PRIV"

	// Table privileges
	SelectPriv   PrivilegeType = "SELECT_PRIV"
	LoadPriv     PrivilegeType = "LOAD_PRIV"
	AlterPriv    PrivilegeType = "ALTER_PRIV"
	CreatePriv   PrivilegeType = "CREATE_PRIV"
	DropPriv     PrivilegeType = "DROP_PRIV"
	UsagePriv    PrivilegeType = "USAGE_PRIV"
	ShowViewPriv PrivilegeType = "SHOW_VIEW_PRIV"
)

// TargetType represents the type of target for privileges
type TargetType string

const (
	// Legacy table targets (catalog.database.table)
	TableTarget TargetType = "table"

	// Resource targets
	ResourceTarget TargetType = "resource"

	// Workload management targets
	WorkloadGroupTarget TargetType = "workload_group"
	ComputeGroupTarget  TargetType = "compute_group"

	// Storage targets
	StorageVaultTarget TargetType = "storage_vault"
)

// TargetFormat stores format information for a target type
type TargetFormat struct {
	FormatString string   // Format string for creating SQL representation
	Keywords     []string // Keywords to identify this target type in a string
}

// TargetTypeFormats maps target types to their SQL representation formats
var TargetTypeFormats = map[TargetType]TargetFormat{
	ResourceTarget: {
		FormatString: "RESOURCE '%s'",
		Keywords:     []string{"RESOURCE"},
	},
	WorkloadGroupTarget: {
		FormatString: "WORKLOAD GROUP '%s'",
		Keywords:     []string{"WORKLOAD GROUP"},
	},
	ComputeGroupTarget: {
		FormatString: "COMPUTE GROUP '%s'",
		Keywords:     []string{"COMPUTE GROUP"},
	},
	StorageVaultTarget: {
		FormatString: "STORAGE VAULT '%s'",
		Keywords:     []string{"STORAGE VAULT"},
	},
	TableTarget: {
		FormatString: "",         // Special case, handled separately
		Keywords:     []string{}, // Default when no other keywords match
	},
}

// Target represents a privilege target in Doris
type Target struct {
	Type TargetType
	Name string
}

// String returns the SQL representation of the target
func (t Target) String() string {
	format, exists := TargetTypeFormats[t.Type]
	if !exists || t.Type == TableTarget {
		// For table targets, ensure proper format (catalog.database.table)
		parts := strings.Split(t.Name, ".")
		for len(parts) < 3 {
			parts = append(parts, "*")
		}
		return strings.Join(parts, ".")
	}
	return fmt.Sprintf(format.FormatString, t.Name)
}

// ParseTargetType extracts the target type from a target string
func ParseTargetType(targetStr string) TargetType {
	for targetType, format := range TargetTypeFormats {
		for _, keyword := range format.Keywords {
			if keyword != "" && strings.Contains(targetStr, keyword) {
				return targetType
			}
		}
	}
	return TableTarget // Default case
}

// Grant represents a single grant in Doris
type Grant struct {
	Privileges []PrivilegeType
	Target     Target
}

// Grant represents the database grant structure
type DBGrant struct {
	// Core fields that are common across versions
	UserIdentity sql.NullString
	Comment      sql.NullString
	Password     sql.NullString
	Roles        sql.NullString

	// Dynamic privilege fields stored as map
	Privileges map[string]sql.NullString
}

// privilegeTypeMap maps Doris privilege column names to their corresponding target types
var privilegeTypeMap = map[string]TargetType{
	// Table-related privileges
	"GlobalPrivs":   TableTarget,
	"CatalogPrivs":  TableTarget,
	"DatabasePrivs": TableTarget,
	"TablePrivs":    TableTarget,
	"ColPrivs":      TableTarget,

	// Resource privileges
	"ResourcePrivs": ResourceTarget,

	// Workload management privileges
	"WorkloadGroupPrivs": WorkloadGroupTarget,
	"ComputeGroupPrivs":  ComputeGroupTarget,

	// Storage privileges
	"StorageVaultPrivs": StorageVaultTarget,

	// Cloud-related privileges (currently not supported)
	// "CloudClusterPrivs":
	// "CloudStagePrivs":
}

// PrivilegeMapping defines the mapping between internal names and SQL names for privileges
type PrivilegeMapping struct {
	// InternalToSQL maps from internal names (used in SHOW GRANTS) to SQL names (used in GRANT/REVOKE)
	InternalToSQL map[string]string
}

// privilegeNameMap maps target types to their privilege mappings
// Only exceptional cases where the names differ are defined here
var privilegeNameMap = map[TargetType]PrivilegeMapping{
	ComputeGroupTarget: {
		InternalToSQL: map[string]string{
			"CLUSTER_USAGE_PRIV": "USAGE_PRIV",
		},
	},
	// Add more exceptional mappings as needed
}

// sqlToInternalName translates a privilege name from SQL form to internal form
func sqlToInternalName(priv string, targetType TargetType) string {
	if mapping, exists := privilegeNameMap[targetType]; exists {
		// Search for the SQL name in the mapping and return its internal name
		for internalName, sqlName := range mapping.InternalToSQL {
			if sqlName == priv {
				return internalName
			}
		}
	}
	return priv
}

// internalToSQLName translates a privilege name from internal form to SQL form
func internalToSQLName(priv string, targetType TargetType) string {
	if mapping, exists := privilegeNameMap[targetType]; exists {
		if mapped, exists := mapping.InternalToSQL[priv]; exists {
			return mapped
		}
	}
	return priv
}

func normalizeColumnOrder(perm string) string {
	re := regexp.MustCompile(`^([^(]*)\((.*)\)$`)
	// We may get inputs like
	// 	SELECT(b,a,c)   -> SELECT(a,b,c)
	// 	DELETE          -> DELETE
	//  SELECT (a,b,c)  -> SELECT(a,b,c)
	// if it's without parentheses, return it right away.
	// Else split what is inside, sort it, concat together and return the result.
	m := re.FindStringSubmatch(perm)
	if m == nil || len(m) < 3 {
		return perm
	}

	parts := strings.Split(m[2], ",")
	for i := range parts {
		parts[i] = strings.Trim(parts[i], "` ")
	}
	sort.Strings(parts)
	precursor := strings.Trim(m[1], " ")
	partsTogether := strings.Join(parts, ", ")
	return fmt.Sprintf("%s(%s)", precursor, partsTogether)
}

func normalizePerms(perms []string) []string {
	ret := []string{}
	for _, perm := range perms {
		// Remove leading and trailing backticks and spaces
		permNorm := strings.Trim(perm, "` ")
		permUcase := strings.ToUpper(permNorm)

		permSortedColumns := normalizeColumnOrder(permUcase)

		ret = append(ret, permSortedColumns)
	}

	// Sort permissions
	sort.Strings(ret)

	return ret
}

func buildGrants(privs sql.NullString, targetType TargetType) ([]mysqlv1alpha1.Grant, error) {
	var grants []mysqlv1alpha1.Grant
	if privs.Valid && privs.String != "" {
		entries := strings.Split(privs.String, ";")
		for _, entry := range entries {
			var target Target = Target{Type: targetType}
			var privileges string
			entryParts := strings.Split(entry, ":")
			if len(entryParts) == 2 {
				target.Name = strings.TrimSpace(entryParts[0])
				privileges = strings.TrimSpace(entryParts[1])
			} else if len(entryParts) == 1 {
				// If no target is specified, use global (*.*.*)
				target.Name = "*.*.*"
				privileges = strings.TrimSpace(entryParts[0])
			} else {
				return nil, fmt.Errorf("invalid privilege format: %s", entry)
			}

			grants = append(grants, mysqlv1alpha1.Grant{
				Privileges: normalizePerms(strings.Split(privileges, ",")),
				Target:     target.String(),
			})
		}
	}
	return grants, nil
}

func (r *MySQLUserReconciler) fetchGrants(ctx context.Context, mysqlClient *sql.DB, userIdentity string) ([]mysqlv1alpha1.Grant, error) {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("userIdentity", userIdentity)

	// Fetch grants for the user
	var grants []mysqlv1alpha1.Grant
	rows, err := mysqlClient.QueryContext(ctx, fmt.Sprintf("SHOW GRANTS FOR %s;", userIdentity))
	if err != nil {
		log.Error(err, "Show grants failed")
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Error(err, "Get columns failed")
		return nil, err
	}

	if rows.Next() {
		var grant DBGrant
		grant.Privileges = make(map[string]sql.NullString)

		// Create scan args dynamically based on columns
		scanArgs := make([]any, len(columns))

		// First 4 fields are always the same
		scanArgs[0] = &grant.UserIdentity
		scanArgs[1] = &grant.Comment
		scanArgs[2] = &grant.Password
		scanArgs[3] = &grant.Roles

		// Remaining fields are privileges
		privPtrs := make(map[string]*sql.NullString)
		for i := 4; i < len(columns); i++ {
			// Only create pointers for columns that exist in privilegeTypeMap
			if _, exists := privilegeTypeMap[columns[i]]; exists {
				priv := new(sql.NullString)
				scanArgs[i] = priv
				privPtrs[columns[i]] = priv
			} else {
				// For unknown columns, use a dummy pointer that we'll ignore
				var dummy sql.NullString
				scanArgs[i] = &dummy
			}
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			log.Error(err, "Scan failed")
			return nil, err
		}

		// Now assign the scanned values to the grant.Privileges map
		for col, ptr := range privPtrs {
			grant.Privileges[col] = *ptr
		}

		// Process each privilege type
		for privType, targetType := range privilegeTypeMap {
			if priv, exists := grant.Privileges[privType]; exists {
				if priv.Valid && priv.String != "" {
					// Split the privileges string and normalize each privilege
					privs := strings.Split(priv.String, ",")
					normalizedPrivs := make([]string, len(privs))
					for i, p := range privs {
						normalizedPrivs[i] = internalToSQLName(strings.TrimSpace(p), targetType)
					}
					priv.String = strings.Join(normalizedPrivs, ",")

					if builtGrants, err := buildGrants(priv, targetType); err != nil {
						log.Error(err, "Build grants failed", "privilegeType", privType, "value", priv)
						return nil, err
					} else {
						grants = append(grants, builtGrants...)
					}
				}
			}
		}
	}

	// Normalize all grants' targets to ensure consistent comparison
	for i := range grants {
		// Ensure table targets follow a consistent format
		if ParseTargetType(grants[i].Target) == TableTarget {
			parts := strings.Split(grants[i].Target, ".")
			for len(parts) < 3 {
				parts = append(parts, "*")
			}
			grants[i].Target = strings.Join(parts, ".")
		}

		// Normalize privileges
		grants[i].Privileges = normalizePerms(grants[i].Privileges)
	}

	return grants, nil
}

func (r *MySQLUserReconciler) grantPrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grant mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("userIdentity", userIdentity)

	// Grant privileges to the user
	_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("GRANT %s ON %s TO %s;", strings.Join(grant.Privileges, ","), grant.Target, userIdentity))

	if err != nil {
		log.Error(err, "Failed to execute GRANT query", "privileges", grant.Privileges, "target", grant.Target)
		return nil
	}
	return nil
}

func (r *MySQLUserReconciler) revokePrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grants []mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("userIdentity", userIdentity)

	// Revoke privileges from the user
	for _, grant := range grants {
		// Get the target type for this grant
		targetType := ParseTargetType(grant.Target)

		// Convert privileges to SQL form for REVOKE statement
		sqlPrivs := make([]string, len(grant.Privileges))
		for i, priv := range grant.Privileges {
			sqlPrivs[i] = internalToSQLName(priv, targetType)
		}

		_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("REVOKE %s ON %s FROM %s;",
			strings.Join(sqlPrivs, ","),
			grant.Target,
			userIdentity))
		if err != nil {
			log.Error(err, "Failed to execute REVOKE query", "privileges", grant.Privileges, "target", grant.Target)
			return err
		}
	}
	return nil
}

func comparePrivileges(oldPrivileges, newPrivileges []string) (revokePrivileges, addPrivileges []string) {
	oldPrivilegeSet := make(map[string]struct{})
	newPrivilegeSet := make(map[string]struct{})

	for _, priv := range oldPrivileges {
		oldPrivilegeSet[priv] = struct{}{}
	}

	for _, priv := range newPrivileges {
		newPrivilegeSet[priv] = struct{}{}
	}

	for priv := range oldPrivilegeSet {
		if _, found := newPrivilegeSet[priv]; !found {
			revokePrivileges = append(revokePrivileges, priv)
		}
	}

	for priv := range newPrivilegeSet {
		if _, found := oldPrivilegeSet[priv]; !found {
			addPrivileges = append(addPrivileges, priv)
		}
	}

	return revokePrivileges, addPrivileges
}
func calculateGrantDiff(oldGrants, newGrants []mysqlv1alpha1.Grant) (grantsToRevoke, grantsToAdd []mysqlv1alpha1.Grant) {
	oldGrantMap := make(map[string]mysqlv1alpha1.Grant)
	newGrantMap := make(map[string]mysqlv1alpha1.Grant)

	// Normalize target for consistent comparison
	normalizeTarget := func(target string) string {
		if ParseTargetType(target) == TableTarget {
			parts := strings.Split(target, ".")
			for len(parts) < 3 {
				parts = append(parts, "*")
			}
			return strings.Join(parts, ".")
		}
		return target
	}

	// Map grants by normalized target
	for _, grant := range oldGrants {
		normalizedTarget := normalizeTarget(grant.Target)
		oldGrantMap[normalizedTarget] = grant
	}

	for _, grant := range newGrants {
		normalizedTarget := normalizeTarget(grant.Target)
		newGrantMap[normalizedTarget] = grant
	}

	// Process targets present in old grants
	for target, oldGrant := range oldGrantMap {
		if newGrant, found := newGrantMap[target]; found {
			// Compare privileges and determine partial revocation and addition
			revokePrivileges, addPrivileges := comparePrivileges(oldGrant.Privileges, newGrant.Privileges)
			if len(revokePrivileges) > 0 {
				grantsToRevoke = append(grantsToRevoke, mysqlv1alpha1.Grant{
					Target:     oldGrant.Target,
					Privileges: revokePrivileges,
				})
			}
			if len(addPrivileges) > 0 {
				grantsToAdd = append(grantsToAdd, mysqlv1alpha1.Grant{
					Target:     newGrant.Target,
					Privileges: addPrivileges,
				})
			}
		} else {
			// If the target doesn't exist in new grants, revoke it completely
			grantsToRevoke = append(grantsToRevoke, oldGrant)
		}
	}

	// Add completely new targets
	for target, newGrant := range newGrantMap {
		if _, found := oldGrantMap[target]; !found {
			grantsToAdd = append(grantsToAdd, newGrant)
		}
	}

	return grantsToRevoke, grantsToAdd
}

func (r *MySQLUserReconciler) updateGrants(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grants []mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("userIdentity", userIdentity)

	// Validate table targets - they must have exactly 3 parts (catalog.database.table)
	for _, grant := range grants {
		targetType := ParseTargetType(grant.Target)
		if targetType == TableTarget {
			parts := strings.Split(grant.Target, ".")
			if len(parts) != 3 {
				errMsg := fmt.Sprintf("Invalid table target format '%s'. Table targets must have exactly 3 parts in the form 'catalog.database.table'", grant.Target)
				log.Error(nil, errMsg)
				return fmt.Errorf("%s", errMsg)
			}

			// Check that none of the parts are empty
			for j, part := range parts {
				if strings.TrimSpace(part) == "" {
					partNames := []string{"catalog", "database", "table"}
					errMsg := fmt.Sprintf("Invalid table target format '%s'. The %s part cannot be empty",
						grant.Target, partNames[j])
					log.Error(nil, errMsg)
					return fmt.Errorf("%s", errMsg)
				}
			}
		}
	}

	// Fetch existing grants
	existingGrants, fetchErr := r.fetchGrants(ctx, mysqlClient, userIdentity)
	if fetchErr != nil {
		log.Error(fetchErr, "Failed to fetch grants")
		return fetchErr
	}

	// Normalize grants
	for i := range grants {
		grants[i].Privileges = normalizePerms(grants[i].Privileges)
	}

	// Calculate grants to revoke and grants to add
	grantsToRevoke, grantsToAdd := calculateGrantDiff(existingGrants, grants)

	// Revoke obsolete grants
	if len(grantsToRevoke) > 0 {
		revokeErr := r.revokePrivileges(ctx, mysqlClient, userIdentity, grantsToRevoke)
		if revokeErr != nil {
			log.Error(revokeErr, "Failed to revoke privileges")
			return revokeErr
		}
		log.Info("User privileges revoked successfully")
	}

	// Grant missing grants
	if len(grantsToAdd) > 0 {
		for _, grant := range grantsToAdd {
			grantErr := r.grantPrivileges(ctx, mysqlClient, userIdentity, grant)
			if grantErr != nil {
				log.Error(grantErr, "Failed to grant privileges")
				return grantErr
			}
		}
		log.Info("User privileges granted successfully")
	}

	return nil
}

// fetchProperties retrieves the current properties for a user using SHOW PROPERTY
func (r *MySQLUserReconciler) fetchProperties(ctx context.Context, mysqlClient *sql.DB, username string) (map[string]string, error) {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("username", username)

	properties := make(map[string]string)

	// Execute SHOW PROPERTY FOR '<username>' to get current properties
	query := fmt.Sprintf("SHOW PROPERTY FOR '%s'", username)
	rows, err := mysqlClient.QueryContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SHOW PROPERTY query")
		return nil, err
	}
	defer rows.Close()

	// Process the results
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Error(err, "Failed to scan property row")
			return nil, err
		}
		properties[key] = value
	}

	if err := rows.Err(); err != nil {
		log.Error(err, "Error iterating over property rows")
		return nil, err
	}

	return properties, nil
}

// setProperty sets a single property for a user using SET PROPERTY
func (r *MySQLUserReconciler) setProperty(ctx context.Context, mysqlClient *sql.DB, username, key, value string) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("username", username, "key", key, "value", value)

	// Execute SET PROPERTY FOR '<username>' '<key>' = '<value>'
	query := fmt.Sprintf("SET PROPERTY FOR '%s' '%s' = '%s'", username, key, value)
	_, err := mysqlClient.ExecContext(ctx, query)
	if err != nil {
		log.Error(err, "Failed to execute SET PROPERTY query")
		return err
	}

	log.Info("Property set successfully")
	return nil
}

// isDefaultValue checks if a value is one of the acceptable default values
func (r *MySQLUserReconciler) isDefaultValue(value string) bool {
	for _, defaultVal := range PropertyDefaultValues {
		if value == defaultVal {
			return true
		}
	}
	return false
}

// setPropertyWithDefaults attempts to set a property value, trying default values if the desired value fails
func (r *MySQLUserReconciler) setPropertyWithDefaults(ctx context.Context, mysqlClient *sql.DB, username, key, desiredValue string) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("username", username, "key", key)

	// First try the desired value
	if err := r.setProperty(ctx, mysqlClient, username, key, desiredValue); err == nil {
		log.Info("Property set with desired value", "value", desiredValue)
		return nil
	} else {
		log.Info("Failed to set desired value, trying defaults", "desiredValue", desiredValue, "error", err.Error())
	}

	// If desired value fails, try default values in order
	// Note: Failures to set individual default values are expected behavior and not considered errors
	for _, defaultValue := range PropertyDefaultValues {
		if err := r.setProperty(ctx, mysqlClient, username, key, defaultValue); err == nil {
			log.Info("Property set with default value", "value", defaultValue)
			return nil
		}
	}

	// If all attempts failed, return the last error
	return fmt.Errorf("failed to set property '%s' with desired value '%s' and all default values", key, desiredValue)
}

// updateProperties manages user properties by comparing desired vs current state
func (r *MySQLUserReconciler) updateProperties(ctx context.Context, mysqlClient *sql.DB, username string, desiredProperties []mysqlv1alpha1.Property) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithValues("username", username)

	// Fetch current properties
	currentProperties, err := r.fetchProperties(ctx, mysqlClient, username)
	if err != nil {
		log.Error(err, "Failed to fetch current properties")
		return err
	}

	// Create a map of desired properties for easier lookup
	desiredPropsMap := make(map[string]string)
	for _, prop := range desiredProperties {
		desiredPropsMap[prop.Name] = prop.Value
	}

	// Update properties that are defined in the spec and have changed
	for _, prop := range desiredProperties {
		if currentValue, exists := currentProperties[prop.Name]; !exists || currentValue != prop.Value {
			log.Info("Property needs update", "key", prop.Name, "currentValue", currentValue, "desiredValue", prop.Value)
			if err := r.setPropertyWithDefaults(ctx, mysqlClient, username, prop.Name, prop.Value); err != nil {
				log.Error(err, "Failed to set property", "key", prop.Name, "value", prop.Value)
				return err
			}
		} else {
			log.V(1).Info("Property already has desired value", "key", prop.Name, "value", prop.Value)
		}
	}

	// Handle unmanaged properties (present in DB but not defined in spec)
	for currentPropName, currentValue := range currentProperties {
		if _, isDefined := desiredPropsMap[currentPropName]; !isDefined {
			// Check if current value is already one of the acceptable default values
			if r.isDefaultValue(currentValue) {
				log.V(1).Info("Unmanaged property already has acceptable default value", "key", currentPropName, "value", currentValue)
				// Do nothing - leave it as is
			} else {
				log.Info("Unmanaged property has non-default value, resetting to default", "key", currentPropName, "currentValue", currentValue)
				if err := r.setPropertyWithDefaults(ctx, mysqlClient, username, currentPropName, ""); err != nil {
					log.Error(err, "Failed to reset unmanaged property to default", "key", currentPropName)
					return err
				}
			}
		}
	}

	log.Info("Properties updated successfully")
	return nil
}
