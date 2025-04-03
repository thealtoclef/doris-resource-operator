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
	"time"

	. "github.com/go-sql-driver/mysql"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	mysqlinternal "github.com/nakamasato/mysql-operator/internal/mysql"
	"github.com/nakamasato/mysql-operator/internal/utils"
)

const mysqlFinalizer = "mysql.nakamasato.com/finalizer"

// MySQLReconciler reconciles a MySQL object
type MySQLReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	MySQLClients    mysqlinternal.MySQLClients
	MySQLDriverName string
}

//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqls,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqls/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqls/finalizers,verbs=update
//+kubebuilder:rbac:groups=mysql.nakamasato.com,resources=mysqlusers,verbs=list;
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MySQL object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *MySQLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithName("MySQLReconciler")

	// Fetch MySQL
	mysql := &mysqlv1alpha1.MySQL{}
	err := r.Get(ctx, req.NamespacedName, mysql)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("[FetchMySQL] Not found", "mysql.Name", mysql.Name, "mysql.Namespace", mysql.Namespace)
			return ctrl.Result{}, nil
		}

		log.Error(err, "[FetchMySQL] Failed to get MySQL")
		return ctrl.Result{}, err
	}

	// Get referenced number
	referencedUserNum, err := r.countReferencesByMySQLUser(ctx, mysql)
	if err != nil {
		log.Error(err, "Failed get referencedUserNum")
		return ctrl.Result{}, err
	}
	log.Info("Successfully got referenced num", "referencedUserNum", referencedUserNum)

	// Update Status
	if mysql.Status.UserCount != int32(referencedUserNum) {
		mysql.Status.UserCount = int32(referencedUserNum)
		err = r.Status().Update(ctx, mysql)
		if err != nil {
			log.Error(err, "[Status] Failed to update status (UserCount)",
				"UserCount", referencedUserNum)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		log.Info("[Status] updated", "UserCount", referencedUserNum)
	}

	// Update MySQLClients
	retry, err := r.UpdateMySQLClients(ctx, mysql)
	if err != nil {
		mysql.Status.Connected = false
		mysql.Status.Reason = err.Error()
		if err := r.Status().Update(ctx, mysql); err != nil {
			log.Error(err, "failed to update status (Connected & Reason)", "status", mysql.Status)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	} else if retry {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	connected, reason := true, "Ping succeded and updated MySQLClients"
	if mysql.Status.Connected != connected || mysql.Status.Reason != reason {
		mysql.Status.Connected = connected
		mysql.Status.Reason = reason
		if err := r.Status().Update(ctx, mysql); err != nil {
			log.Error(err, "failed to update status (Connected & Reason)", "status", mysql.Status)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	// Handle finalizer
	finalizerResult, finalizerErr := utils.HandleFinalizer(utils.FinalizerParams{
		Object:    mysql,
		Context:   ctx,
		Client:    r.Client,
		Finalizer: mysqlFinalizer,
		FinalizationFunc: func() error {
			if r.finalizeMySQL(ctx, mysql) {
				return nil
			} else {
				log.Info("Could not complete finalizer. waiting another second")
				return fmt.Errorf("finalizer could not complete")
			}
		},
	})

	if finalizerErr != nil || !mysql.GetDeletionTimestamp().IsZero() {
		if finalizerErr != nil {
			// Requeue after 1 second if finalization failed
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		// Otherwise return the result from finalizer handling
		return finalizerResult, finalizerErr
	}

	// If there is no finalizer processing or deletion, proceed with normal reconciliation
	return ctrl.Result{RequeueAfter: constants.ReconciliationPeriod}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MySQLReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.MySQL{}).
		Owns(&mysqlv1alpha1.MySQLUser{}).
		Complete(r)
}

func (r *MySQLReconciler) UpdateMySQLClients(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (retry bool, err error) {
	log := log.FromContext(ctx).WithName("MySQLReconciler")
	// Get MySQL config from raw username and password or GCP secret manager
	cfg, err := r.getMySQLConfig(ctx, mysql)
	if err != nil {
		return true, err
	}
	if db, _ := r.MySQLClients.GetClient(mysql.GetKey()); db == nil {
		log.Info("MySQLClients doesn't have client", "key", mysql.GetKey())

		db, err := sql.Open(r.MySQLDriverName, cfg.FormatDSN())
		if err != nil {
			log.Error(err, "Failed to open MySQL database", "mysql.Name", mysql.Name)
			return true, err
		}
		err = db.PingContext(ctx)
		if err != nil {
			log.Error(err, "Ping failed", "mysql.Name", mysql.Name)
			return true, err
		}

		// key: mysql.Namespace-mysql.Name
		r.MySQLClients[mysql.GetKey()] = db
		log.Info("Successfully added MySQL client", "mysql.Name", mysql.Name)
	}
	return false, nil
}

func (r *MySQLReconciler) getMySQLConfig(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (Config, error) {
	log := log.FromContext(ctx)

	// Get credentials from Kubernetes secret referenced by authSecret
	secret := &v1.Secret{}
	authSecretName := mysql.Spec.AuthSecret

	// Get the secret from the same namespace as the MySQL CR
	err := r.Get(ctx, client.ObjectKey{Namespace: mysql.Namespace, Name: authSecretName}, secret)
	if err != nil {
		log.Error(err, "failed to get kubernetes secret", "secret", authSecretName)
		return Config{}, err
	}

	// Extract username and password from the secret
	username, ok := secret.Data["username"]
	if !ok {
		return Config{}, fmt.Errorf("secret %s is missing username key", authSecretName)
	}

	password, ok := secret.Data["password"]
	if !ok {
		return Config{}, fmt.Errorf("secret %s is missing password key", authSecretName)
	}

	// Log success but not the actual credentials
	log.Info("Successfully retrieved MySQL credentials", "secret", authSecretName)

	return Config{
		User:                 string(username),
		Passwd:               string(password),
		Addr:                 fmt.Sprintf("%s:%d", mysql.Spec.Host, mysql.Spec.Port),
		Net:                  "tcp",
		AllowNativePasswords: true,
	}, nil
}

func (r *MySQLReconciler) countReferencesByMySQLUser(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (int, error) {
	// 1. Get the referenced MySQLUser instances.
	// 2. Return the number of referencing MySQLUser.
	mysqlUserList := &mysqlv1alpha1.MySQLUserList{}
	err := r.List(ctx, mysqlUserList, client.MatchingFields{"spec.clusterName": mysql.Name})

	if err != nil {
		return 0, err
	}
	return len(mysqlUserList.Items), nil
}

// finalizeMySQL return true if no user is referencing the given MySQL
func (r *MySQLReconciler) finalizeMySQL(ctx context.Context, mysql *mysqlv1alpha1.MySQL) bool {
	log := log.FromContext(ctx).WithName("MySQLReconciler")
	if mysql.Status.UserCount > 0 {
		log.Info("there's referencing user", "UserCount", mysql.Status.UserCount)
		return false
	}
	if db, ok := r.MySQLClients[mysql.GetKey()]; ok {
		if err := db.Close(); err != nil {
			return false
		}
		delete(r.MySQLClients, mysql.GetKey())
		log.Info("Closed and removed MySQL client", "mysql.Key", mysql.GetKey())
	} else {
		log.Info("MySQL client doesn't exist", "mysql.Key", mysql.GetKey())
	}
	return true
}
