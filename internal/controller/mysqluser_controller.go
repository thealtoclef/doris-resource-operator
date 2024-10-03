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
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const (
	mysqlUserFinalizer                         = "mysqluser.nakamasato.com/finalizer"
	mysqlUserReasonCompleted                   = "User and Secret are successfully created. Grants are successfully updated."
	mysqlUserReasonMySQLConnectionFailed       = "Failed to connect to MySQL"
	mysqlUserReasonMySQLFailedToCreateUser     = "Failed to create User"
	mysqlUserReasonMySQLFailedToUpdatePassword = "Failed to update password of User"
	mysqlUserReasonMySQLFailedToCreateSecret   = "Failed to create Secret"
	mysqlUserReasonMYSQLFailedToGrantPrivs     = "Failed to grant Privileges"
	mysqlUserReasonMySQLFetchFailed            = "Failed to fetch MySQL"
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
	mysqlUserName := mysqlUser.Name
	mysqlUserHost := mysqlUser.Spec.Host
	mysqlName := mysqlUser.Spec.MysqlName
	mysqlUserGrants := mysqlUser.Spec.Grants

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	var mysqlNamespacedName = client.ObjectKey{Namespace: req.Namespace, Name: mysqlUser.Spec.MysqlName}
	if err := r.Get(ctx, mysqlNamespacedName, mysql); err != nil {
		log.Error(err, "[FetchMySQL] Failed")
		mysqlUser.Status.Phase = mysqlUserPhaseNotReady
		mysqlUser.Status.Reason = mysqlUserReasonMySQLFetchFailed
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
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
		log.Error(err, "Failed to get MySQL client", "key", mysql.GetKey())
		return ctrl.Result{}, err
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

	// Get password from Secret if exists. Otherwise, generate new one.
	secretName := getSecretName(mysqlName, mysqlUserName)
	secret := &v1.Secret{}
	err = r.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: secretName}, secret)
	var password string
	if err != nil {
		if errors.IsNotFound(err) { // Secret doesn't exists -> generate password
			log.Info("[password] Generate new password for Secret", "secretName", secretName)
			password = utils.GenerateRandomString(16)
			// Create Secret with the password
			err = r.createSecret(ctx, password, secretName, mysqlUser.Namespace, mysqlUser)
			if err != nil {
				log.Error(err, "Failed to create Secret", "secretName", secretName, "namespace", mysqlUser.Namespace, "mysqlUser", mysqlUser.Name)
				mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToCreateSecret
				mysqlUser.Status.SecretCreated = false
				if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
					log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
					return ctrl.Result{RequeueAfter: time.Second}, nil
				}
				return ctrl.Result{}, err
			}
		} else {
			log.Error(err, "[password] Failed to get Secret", "secretName", secretName)
			return ctrl.Result{}, err // requeue
		}
	} else { // exists -> get password from Secret
		log.Info("[password] Get password from Secret", "secretName", secretName)
		password = string(secret.Data["password"])
	}
	mysqlUser.Status.SecretCreated = true

	// Check if MySQL user exists
	userIdentity := fmt.Sprintf("'%s'@'%s'", strings.ReplaceAll(mysqlUserName, "-", "_"), mysqlUserHost)
	_, err = mysqlClient.ExecContext(ctx, fmt.Sprintf("SHOW GRANTS FOR %s", userIdentity))
	if err != nil {
		// Create User if not exists with the password set above.
		_, err = mysqlClient.ExecContext(ctx,
			fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'", userIdentity, password))
		if err != nil {
			log.Error(err, "[MySQL] Failed to create User", "mysqlName", mysqlName, "mysqlUserName", mysqlUserName)
			mysqlUser.Status.Phase = mysqlUserPhaseNotReady
			mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToCreateUser
			mysqlUser.Status.UserCreated = false
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		log.Info("[MySQL] Created User", "mysqlName", mysqlName, "mysqlUserName", mysqlUserName)
		metrics.MysqlUserCreatedTotal.Increment() // TODO: increment only when a user is created
	} else {
		// Update password of User if already exists with the password set above.
		_, err = mysqlClient.ExecContext(ctx,
			fmt.Sprintf("ALTER USER %s IDENTIFIED BY '%s'", userIdentity, password))
		if err != nil {
			log.Error(err, "[MySQL] Failed to update password of User", "mysqlName", mysqlName, "mysqlUserName", mysqlUserName)
			mysqlUser.Status.Phase = mysqlUserPhaseNotReady
			mysqlUser.Status.Reason = mysqlUserReasonMySQLFailedToUpdatePassword
			mysqlUser.Status.PasswordUpdated = false
			if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
				log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil // requeue after 1 second
		}
		log.Info("[MySQL] Updated password of User", "mysqlName", mysqlName, "mysqlUserName", mysqlUserName)
	}
	mysqlUser.Status.UserIdentity = userIdentity
	mysqlUser.Status.UserCreated = true
	mysqlUser.Status.PasswordUpdated = true

	// Update Grants
	err = r.updateGrants(ctx, mysqlClient, userIdentity, mysqlUserGrants)
	if err != nil {
		log.Error(err, "[MySQL] Failed to update Grants", "mysqlName", mysqlName, "mysqlUserName", mysqlUserName)
		mysqlUser.Status.Reason = mysqlUserReasonMYSQLFailedToGrantPrivs
		if serr := r.Status().Update(ctx, mysqlUser); serr != nil {
			log.Error(serr, "Failed to update MySQLUser status", "mysqlUser", mysqlUser.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	mysqlUser.Status.GrantsUpdated = true

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
		_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", mysqlUser.Status.UserIdentity))
		if err != nil {
			return err
		}
		metrics.MysqlUserDeletedTotal.Increment()
	}

	return nil
}

func getSecretName(mysqlName string, mysqlUserName string) string {
	str := []string{mysqlName, mysqlUserName}
	return strings.Join(str, "-")
}

func (r *MySQLUserReconciler) createSecret(ctx context.Context, password string, secretName string, namespace string, mysqlUser *mysqlv1alpha1.MySQLUser) error {
	log := log.FromContext(ctx)
	data := make(map[string][]byte)
	data["password"] = []byte(password)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
	}
	err := ctrl.SetControllerReference(mysqlUser, secret, r.Scheme) // Set owner of this Secret
	if err != nil {
		log.Error(err, "Failed to SetControllerReference for Secret.")
		return err
	}
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Data = data
		log.Info("Successfully created Secret.")
		return nil
	}); err != nil {
		log.Error(err, "Error creating or updating Secret.")
		return err
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

type DorisGrant struct {
	UserIdentity       sql.NullString
	Comment            sql.NullString
	Password           sql.NullString
	Roles              sql.NullString
	GlobalPrivs        sql.NullString
	CatalogPrivs       sql.NullString
	DatabasePrivs      sql.NullString
	TablePrivs         sql.NullString
	ColPrivs           sql.NullString
	ResourcePrivs      sql.NullString
	WorkloadGroupPrivs sql.NullString
}

type EntityType string

const (
	Table         EntityType = "table"
	Resource      EntityType = "resource"
	WorkloadGroup EntityType = "workload_group"
)

func (t EntityType) Equals(other EntityType) bool {
	return t == other
}

type Entity struct {
	Type EntityType
	Name string
}

func (e Entity) IDString() string {
	return fmt.Sprintf("%s:%s", e.Type, e.Name)
}

func (e Entity) SQLString() string {
	switch e.Type {
	case Resource:
		return fmt.Sprintf("RESOURCE '%s'", e.Name)
	case WorkloadGroup:
		return fmt.Sprintf("WORKLOAD GROUP '%s'", e.Name)
	default:
		return e.Name
	}
}

func (e Entity) Equals(other Entity) bool {
	return e.Type == other.Type && e.Name == other.Name
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

func buildGrants(privs sql.NullString, entityType EntityType) ([]mysqlv1alpha1.Grant, error) {
	var grants []mysqlv1alpha1.Grant
	if privs.Valid && privs.String != "" {
		entries := strings.Split(privs.String, ";")
		for _, entry := range entries {
			var entity Entity = Entity{Type: entityType}
			var privileges string
			entryParts := strings.Split(entry, ":")
			if len(entryParts) == 2 {
				entity.Name = strings.TrimSpace(entryParts[0])
				privileges = strings.TrimSpace(entryParts[1])
			} else if len(entryParts) == 1 {
				// If no target is specified, use global (*.*.*)
				entity.Name = "*.*.*"
				privileges = strings.TrimSpace(entryParts[0])
			} else {
				return nil, fmt.Errorf("invalid privilege format: %s", entry)
			}

			// Ensure that entity.Name matches the format *.*.* for the Table entity type
			if entityType == Table {
				nameParts := strings.Split(strings.TrimSpace(entity.Name), ".")
				for len(nameParts) < 3 {
					nameParts = append(nameParts, "*")
				}
				entity.Name = strings.Join(nameParts, ".")
			}

			grants = append(grants, mysqlv1alpha1.Grant{
				Privileges: normalizePerms(strings.Split(privileges, ",")),
				Target:     entity.SQLString(),
			})
		}
	}
	return grants, nil
}

func grantKey(grant mysqlv1alpha1.Grant) string {
	return fmt.Sprintf("%s:%s", strings.Join(grant.Privileges, ","), grant.Target)
}

func calculateGrantDiff(oldGrants, newGrants []mysqlv1alpha1.Grant) (grantsToRevoke, grantsToAdd []mysqlv1alpha1.Grant) {
	oldGrantMap := make(map[string]mysqlv1alpha1.Grant)
	newGrantMap := make(map[string]mysqlv1alpha1.Grant)

	for _, grant := range oldGrants {
		oldGrantMap[grantKey(grant)] = grant
	}

	for _, grant := range newGrants {
		newGrantMap[grantKey(grant)] = grant
	}

	for key, existingGrant := range oldGrantMap {
		if _, found := newGrantMap[key]; !found {
			grantsToRevoke = append(grantsToRevoke, existingGrant)
		}
	}

	for key, inputGrant := range newGrantMap {
		if _, found := oldGrantMap[key]; !found {
			grantsToAdd = append(grantsToAdd, inputGrant)
		}
	}

	return grantsToRevoke, grantsToAdd
}

func fetchExistingGrants(ctx context.Context, mysqlClient *sql.DB, userIdentity string) ([]mysqlv1alpha1.Grant, error) {
	var grants []mysqlv1alpha1.Grant

	log := log.FromContext(ctx)
	rows, err := mysqlClient.QueryContext(ctx, fmt.Sprintf("SHOW GRANTS FOR %s;", userIdentity))
	if err != nil {
		log.Error(err, "[DorisUserGrant] Show grants failed: %w", err)
		return nil, err
	}

	defer rows.Close()

	if rows.Next() {
		var dorisGrant DorisGrant
		err := rows.Scan(
			&dorisGrant.UserIdentity, &dorisGrant.Comment, &dorisGrant.Password, &dorisGrant.Roles, &dorisGrant.GlobalPrivs,
			&dorisGrant.CatalogPrivs, &dorisGrant.DatabasePrivs, &dorisGrant.TablePrivs, &dorisGrant.ColPrivs,
			&dorisGrant.ResourcePrivs, &dorisGrant.WorkloadGroupPrivs,
		)
		if err != nil {
			log.Error(err, "[DorisUserGrant] Read row failed: %w", err)
			return nil, err
		}

		entries := []struct {
			privs      sql.NullString
			entityType EntityType
		}{
			{dorisGrant.GlobalPrivs, Table},
			{dorisGrant.CatalogPrivs, Table},
			{dorisGrant.DatabasePrivs, Table},
			{dorisGrant.TablePrivs, Table},
			{dorisGrant.ColPrivs, Table},
			{dorisGrant.ResourcePrivs, Resource},
			{dorisGrant.WorkloadGroupPrivs, WorkloadGroup},
		}

		for _, entry := range entries {
			if builtGrants, err := buildGrants(entry.privs, entry.entityType); err != nil {
				log.Error(err, "[DorisUserGrant] Build grants failed: %w", err)
				return nil, err
			} else {
				grants = append(grants, builtGrants...)
			}
		}
	}
	return grants, nil
}

func (r *MySQLUserReconciler) revokePrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grants []mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx)
	for _, grant := range grants {
		_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("REVOKE %s ON %s FROM %s;", strings.Join(grant.Privileges, ","), grant.Target, userIdentity))
		if err != nil {
			log.Error(err, "[DorisUserGrant] Revoke failed: %w", err)
			return err
		}
		log.Info("[DorisUserGrant] Revoke", "userIdentity", userIdentity, "grant.Privileges", grant.Privileges, "on", grant.Target)
	}
	return nil
}

func (r *MySQLUserReconciler) grantPrivileges(ctx context.Context, mysqlClient *sql.DB, userIdentity string, grant mysqlv1alpha1.Grant) error {
	log := log.FromContext(ctx)
	_, err := mysqlClient.ExecContext(ctx, fmt.Sprintf("GRANT %s ON %s TO %s;", strings.Join(grant.Privileges, ","), grant.Target, userIdentity))
	if err != nil {
		return err
	}
	log.Info("[DorisUserGrant] Grant", "userIdentity", userIdentity, "grant.Privileges", grant.Privileges, "on", grant.Target)
	return nil
}

func (r *MySQLUserReconciler) updateGrants(ctx context.Context, mysqlClient *sql.DB, userIdentity string, mysqlUserGrants []mysqlv1alpha1.Grant) error {
	// Fetch existing grants
	existingGrants, fetchErr := fetchExistingGrants(ctx, mysqlClient, userIdentity)
	if fetchErr != nil {
		return fetchErr
	}

	// Normalize grants
	for i := range mysqlUserGrants {
		mysqlUserGrants[i].Privileges = normalizePerms(mysqlUserGrants[i].Privileges)
	}

	// Calculate grants to revoke and grants to add
	grantsToRevoke, grantsToAdd := calculateGrantDiff(existingGrants, mysqlUserGrants)

	// Revoke obsolete grants
	revokeErr := r.revokePrivileges(ctx, mysqlClient, userIdentity, grantsToRevoke)
	if revokeErr != nil {
		return revokeErr
	}

	// Grant missing grants
	for _, grant := range grantsToAdd {
		grantErr := r.grantPrivileges(ctx, mysqlClient, userIdentity, grant)
		if grantErr != nil {
			return grantErr
		}
	}

	return nil
}
