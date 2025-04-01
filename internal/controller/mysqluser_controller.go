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
	"github.com/nakamasato/mysql-operator/internal/metrics"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
)

const (
	mysqlUserFinalizer                         = "mysqluser.nakamasato.com/finalizer"
	mysqlUserReasonCompleted                   = "User are successfully reconciled"
	mysqlUserReasonMySQLConnectionFailed       = "Failed to connect to cluster"
	mysqlUserReasonMySQLFailedToCreateUser     = "Failed to create user"
	mysqlUserReasonMySQLFailedToUpdatePassword = "Failed to update password"
	mysqlUserReasonMySQLFailedToGetSecret      = "Failed to get Secret"
	mysqlUserReasonMYSQLFailedToGrant          = "Failed to grant"
	mysqlUserReasonMySQLFetchFailed            = "Failed to fetch cluster"
	mysqlUserPhaseReady                        = "Ready"
	mysqlUserPhaseNotReady                     = "NotReady"
)

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

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: clusterName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		mysqlUser.Status.Phase = mysqlUserPhaseNotReady
		mysqlUser.Status.Reason = mysqlUserReasonMySQLFetchFailed
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
		mysqlUser.Status.Phase = mysqlUserPhaseNotReady
		mysqlUser.Status.Reason = mysqlUserReasonMySQLConnectionFailed
		log.Error(err, "[MySQLClient] Failed to connect to cluster", "key", mysql.GetKey(), "clusterName", clusterName)
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, err //requeue
	}
	log.Info("[MySQLClient] Successfully connected")

	// Finalize if DeletionTimestamp exists
	if !mysqlUser.GetDeletionTimestamp().IsZero() {
		log.Info("isMysqlUserMarkedToBeDeleted is true")
		if controllerutil.ContainsFinalizer(mysqlUser, mysqlUserFinalizer) {
			log.Info("ContainsFinalizer is true")
			// Run finalization logic for mysqlUserFinalizer. If the
			// finalization logic fails, don't remove the finalizer so
			// that we can retry during the next reconciliation.
			if err := r.finalizeMySQLUser(ctx, mysqlClient, mysqlUser); err != nil {
				log.Error(err, "Failed to complete finalizeMySQLUser")
				return ctrl.Result{}, err
			}
			log.Info("finalizeMySQLUser completed")
			// Remove mysqlUserFinalizer. Once all finalizers have been
			// removed, the object will be deleted.
			log.Info("removing finalizer")
			if controllerutil.RemoveFinalizer(mysqlUser, mysqlUserFinalizer) {
				log.Info("RemoveFinalizer completed")
				err := r.Update(ctx, mysqlUser)
				log.Info("Update")
				if err != nil {
					log.Error(err, "Failed to update mysqlUser")
					return ctrl.Result{}, err
				}
				log.Info("Update completed")
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil // should return success when not having the finalizer
	}

	// Add finalizer for this CR
	log.Info("Add Finalizer for this CR")
	if controllerutil.AddFinalizer(mysqlUser, mysqlUserFinalizer) {
		log.Info("Added Finalizer")
		err = r.Update(ctx, mysqlUser)
		if err != nil {
			log.Info("Failed to update after adding finalizer")
			return ctrl.Result{}, err // requeue
		}
		log.Info("Updated successfully after adding finalizer")
	} else {
		log.Info("already has finalizer")
	}

	// Skip all the following steps if MySQL is being Deleted
	if !mysql.GetDeletionTimestamp().IsZero() {
		log.Info("MySQL is being deleted. MySQLUser cannot be created.", "mysql", mysql.Name, "mysqlUser", mysqlUser.Name)
		return ctrl.Result{}, err
	}

	// Get password from Secret
	secret := &v1.Secret{}
	err = r.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: passwordSecretRef.Name}, secret)
	if err != nil {
		log.Error(err, "[password] Failed to get Secret", "passwordSecretRef", passwordSecretRef)
		mysqlUser.Status.Phase = mysqlUserPhaseNotReady
		mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToGetSecret
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
			mysqlUser.Status.Phase = mysqlUserPhaseNotReady
			mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToCreateUser
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
			mysqlUser.Status.Phase = mysqlUserPhaseNotReady
			mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToUpdatePassword
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
			}
			return ctrl.Result{}, err //requeue
		}
		log.Info("[MySQL] Updated password of User", "clusterName", clusterName, "userIdentity", userIdentity)
	}

	// Update Grants
	err = r.updateGrants(ctx, mysqlClient, userIdentity, grants)
	if err != nil {
		log.Error(err, "[MySQL] Failed to update Grants", "clusterName", clusterName, "userIdentity", userIdentity)
		mysqlUser.Status.Phase = mysqlUserPhaseNotReady
		mysqlUser.Status.Reason = mysqlUserReasonMYSQLFailedToGrant
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		return ctrl.Result{}, err
	}
	// Update phase and reason of MySQLUser status to Ready and Completed
	mysqlUser.Status.Phase = mysqlUserPhaseReady
	mysqlUser.Status.Reason = mysqlUserReasonCompleted
	if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
		log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
	}

	return ctrl.Result{}, nil
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

	// Cloud-related privileges (all treated as table privileges)
	"CloudClusterPrivs": TableTarget,
	"CloudStagePrivs":   TableTarget,
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
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithName("FetchGrants").WithValues("userIdentity", userIdentity)

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
			priv := new(sql.NullString)
			scanArgs[i] = priv
			privPtrs[columns[i]] = priv
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

		log.Info("Scanned", "grant", grant)

		// Process each privilege type
		for privType, targetType := range privilegeTypeMap {
			if priv, exists := grant.Privileges[privType]; exists {
				if priv.Valid && priv.String != "" {
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

	log.Info("Fetched grants", "count", len(grants), "grants", grants)
	return grants, nil
}

func (r *MySQLUserReconciler) grantPrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grant mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithName("GrantPrivileges").WithValues("userIdentity", userIdentity)

	// Grant privileges to the user
	_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("GRANT %s ON %s TO %s;", strings.Join(grant.Privileges, ","), grant.Target, userIdentity))

	if err != nil {
		log.Error(err, "Failed", "privileges", grant.Privileges, "target", grant.Target)
		return nil
	}
	log.Info("Granted", "privileges", grant.Privileges, "target", grant.Target)
	return nil
}

func (r *MySQLUserReconciler) revokePrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grants []mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithName("RevokePrivileges").WithValues("userIdentity", userIdentity)

	// Revoke privileges from the user
	for _, grant := range grants {
		_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("REVOKE %s ON %s FROM %s;", strings.Join(grant.Privileges, ","), grant.Target, userIdentity))
		if err != nil {
			log.Error(err, "Failed", "privileges", grant.Privileges, "target", grant.Target)
			return err
		}
		log.Info("Revoked", "privileges", grant.Privileges, "target", grant.Target)
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
	log := log.FromContext(ctx).WithName("MySQLUserReconciler").WithName("UpdateGrants").WithValues("userIdentity", userIdentity)

	// Fetch grants
	existingGrants, fetchErr := r.fetchGrants(ctx, mysqlClient, userIdentity)
	if fetchErr != nil {
		log.Error(fetchErr, "Failed to fetch grants")
		return fetchErr
	}
	log.Info("Existing grants", "count", len(existingGrants), "grants", existingGrants)

	// Normalize grants
	log.Info("Normalizing desired grants", "count", len(grants))
	for i := range grants {
		originalPrivs := grants[i].Privileges
		grants[i].Privileges = normalizePerms(grants[i].Privileges)
		log.Info("Normalized grant privileges",
			"target", grants[i].Target,
			"before", originalPrivs,
			"after", grants[i].Privileges)
	}

	// Calculate grants to revoke and grants to add
	grantsToRevoke, grantsToAdd := calculateGrantDiff(existingGrants, grants)
	log.Info("Grants to revoke", "count", len(grantsToRevoke), "grants", grantsToRevoke)
	log.Info("Grants to add", "count", len(grantsToAdd), "grants", grantsToAdd)

	// Revoke obsolete grants
	if len(grantsToRevoke) > 0 {
		log.Info("Revoking obsolete grants")
		revokeErr := r.revokePrivileges(ctx, mysqlClient, userIdentity, grantsToRevoke)
		if revokeErr != nil {
			log.Error(revokeErr, "Failed to revoke privileges")
			return revokeErr
		}
	} else {
		log.Info("No grants to revoke")
	}

	// Grant missing grants
	if len(grantsToAdd) > 0 {
		log.Info("Adding new grants")
		for _, grant := range grantsToAdd {
			grantErr := r.grantPrivileges(ctx, mysqlClient, userIdentity, grant)
			if grantErr != nil {
				log.Error(grantErr, "Failed to grant privileges")
				return grantErr
			}
		}
	} else {
		log.Info("No grants to add")
	}

	return nil
}
