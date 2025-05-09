package controllers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/nakamasato/mysql-operator/api/v1alpha1"
	"github.com/nakamasato/mysql-operator/internal/constants"
	. "github.com/nakamasato/mysql-operator/internal/mysql"
)

var _ = Describe("MySQLUser controller", func() {

	Context("With available MySQL", func() {

		ctx := context.Background()
		var stopFunc func()
		var close func() error
		BeforeEach(func() {
			k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme: scheme,
			})
			Expect(err).ToNot(HaveOccurred())
			db, err := sql.Open("testdbdriver", "test")
			close = db.Close
			Expect(err).ToNot(HaveOccurred())
			reconciler := &MySQLUserReconciler{
				Client:       k8sManager.GetClient(),
				Scheme:       k8sManager.GetScheme(),
				MySQLClients: MySQLClients{fmt.Sprintf("%s-%s", Namespace, MySQLName): db},
			}
			err = ctrl.NewControllerManagedBy(k8sManager).
				For(&mysqlv1alpha1.MySQLUser{}).
				Named(fmt.Sprintf("mysqluser-test-%d", time.Now().UnixNano())).
				Complete(reconciler)
			Expect(err).ToNot(HaveOccurred())

			ctx, cancel := context.WithCancel(ctx)
			stopFunc = cancel
			go func() {
				err = k8sManager.Start(ctx)
				Expect(err).ToNot(HaveOccurred())
			}()
			time.Sleep(100 * time.Millisecond)
		})

		AfterEach(func() {
			stopFunc()
			err := close()
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(100 * time.Millisecond)
		})

		When("Creating a MySQLUser", func() {
			AfterEach(func() {
				// Delete MySQLUser
				cleanUpMySQLUser(ctx, k8sClient, Namespace)
				// Delete MySQL
				cleanUpMySQL(ctx, k8sClient, Namespace)
			})
			It("Should create Secret and update mysqluser's status", func() {
				By("By creating a new MySQL")
				mysql = &mysqlv1alpha1.MySQL{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQL"},
					ObjectMeta: metav1.ObjectMeta{Name: MySQLName, Namespace: Namespace},
					Spec:       mysqlv1alpha1.MySQLSpec{Host: "nonexistinghost", AuthSecret: "mysql-auth"},
				}
				Expect(k8sClient.Create(ctx, mysql)).Should(Succeed())

				By("By creating a new MySQLUser")
				mysqlUser = &mysqlv1alpha1.MySQLUser{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQLUser"},
					ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: MySQLUserName},
					Spec:       mysqlv1alpha1.MySQLUserSpec{ClusterName: MySQLName},
					Status:     mysqlv1alpha1.MySQLUserStatus{},
				}
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				// secret should be created
				secret := &v1.Secret{}
				Eventually(func() error {
					return k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: mysqlUser.Spec.PasswordSecretRef.Name}, secret)
				}).Should(Succeed())

				// status.phase should be ready
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Phase
				}).Should(Equal(constants.PhaseReady))

				// status.reason should be 'both secret and mysql user are successfully created.'
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Reason
				}).Should(Equal(constants.ReasonCompleted))
			})

			It("Should have finalizer", func() {
				By("By creating a new MySQL")
				mysql = &mysqlv1alpha1.MySQL{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQL"},
					ObjectMeta: metav1.ObjectMeta{Name: MySQLName, Namespace: Namespace},
					Spec:       mysqlv1alpha1.MySQLSpec{Host: "nonexistinghost", AuthSecret: "mysql-auth"},
				}
				Expect(k8sClient.Create(ctx, mysql)).Should(Succeed())
				By("By creating a new MySQLUser")
				mysqlUser = &mysqlv1alpha1.MySQLUser{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQLUser"},
					ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: MySQLUserName},
					Spec:       mysqlv1alpha1.MySQLUserSpec{ClusterName: MySQLName},
					Status:     mysqlv1alpha1.MySQLUserStatus{},
				}
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				Eventually(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return false
					}
					return controllerutil.ContainsFinalizer(mysqlUser, mysqlUserFinalizer)
				}).Should(BeTrue())
			})
		})

		When("Deleting a MySQLUser", func() {
			BeforeEach(func() {
				// Clean up MySQLUser
				cleanUpMySQLUser(ctx, k8sClient, Namespace)
				// Clean up MySQL
				cleanUpMySQL(ctx, k8sClient, Namespace)
				// Clean up Secret
				cleanUpSecret(ctx, k8sClient, Namespace)

				// Create resources
				mysql = &mysqlv1alpha1.MySQL{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQL"},
					ObjectMeta: metav1.ObjectMeta{Name: MySQLName, Namespace: Namespace},
					Spec:       mysqlv1alpha1.MySQLSpec{Host: "nonexistinghost", AuthSecret: "mysql-auth"},
				}
				Expect(k8sClient.Create(ctx, mysql)).Should(Succeed())

				mysqlUser = &mysqlv1alpha1.MySQLUser{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQLUser"},
					ObjectMeta: metav1.ObjectMeta{Name: MySQLUserName, Namespace: Namespace},
					Spec:       mysqlv1alpha1.MySQLUserSpec{ClusterName: MySQLName},
					Status:     mysqlv1alpha1.MySQLUserStatus{},
				}
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())
			})
			AfterEach(func() {
				// Delete MySQL
				Expect(k8sClient.Delete(ctx, mysql)).Should(Succeed())
				// Remove finalizers from MySQL if exists
				if k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLName}, mysql) == nil {
					mysql.Finalizers = []string{}
					Eventually(k8sClient.Update(ctx, mysql)).Should(Succeed())
				}
				Eventually(func() error {
					return k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLName}, mysql)
				}).ShouldNot(Succeed())
			})
			It("Should delete Secret", func() {

				By("By deleting a MySQLUser")
				mysqlUser = &mysqlv1alpha1.MySQLUser{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQLUser"},
					ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: MySQLUserName},
					Spec:       mysqlv1alpha1.MySQLUserSpec{ClusterName: MySQLName},
					Status:     mysqlv1alpha1.MySQLUserStatus{},
				}
				Expect(k8sClient.Delete(ctx, mysqlUser)).To(Succeed())

				mysqlUser = &mysqlv1alpha1.MySQLUser{}
				Eventually(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					return errors.IsNotFound(err) // MySQLUser should not exist
				}).Should(BeTrue())

				secret := &v1.Secret{}
				secretName := mysqlUser.Spec.PasswordSecretRef.Name
				Eventually(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: secretName}, secret)
					return errors.IsNotFound(err) // Secret should not exist
				}).Should(BeTrue())

				// MySQL should remain
				mysql = &mysqlv1alpha1.MySQL{}
				Consistently(func() error {
					return k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLName}, mysql)
				}).Should(Succeed())
			})

		})
	})

	Context("With MySQL with unnconnectable configuration", func() {
		ctx := context.Background()
		var stopFunc func()
		var close func() error
		BeforeEach(func() {
			k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme: scheme,
			})
			Expect(err).ToNot(HaveOccurred())
			db, err := sql.Open("mysql", "test_user:password@tcp(nonexistinghost:3306)/")
			Expect(err).NotTo(HaveOccurred())
			close = db.Close
			reconciler := &MySQLUserReconciler{
				Client:       k8sManager.GetClient(),
				Scheme:       k8sManager.GetScheme(),
				MySQLClients: MySQLClients{fmt.Sprintf("%s-%s", Namespace, MySQLName): db},
			}
			err = ctrl.NewControllerManagedBy(k8sManager).
				For(&mysqlv1alpha1.MySQLUser{}).
				Named(fmt.Sprintf("mysqluser-test-%d", time.Now().UnixNano())).
				Complete(reconciler)
			Expect(err).ToNot(HaveOccurred())

			ctx, cancel := context.WithCancel(ctx)
			stopFunc = cancel
			go func() {
				err = k8sManager.Start(ctx)
				Expect(err).ToNot(HaveOccurred())
			}()
			time.Sleep(100 * time.Millisecond)

			By("By creating a new MySQL")
			mysql = &mysqlv1alpha1.MySQL{
				TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQL"},
				ObjectMeta: metav1.ObjectMeta{Name: MySQLName, Namespace: Namespace},
				Spec:       mysqlv1alpha1.MySQLSpec{Host: "nonexistinghost", AuthSecret: "mysql-auth"},
			}
			Expect(k8sClient.Create(ctx, mysql)).Should(Succeed())
		})

		AfterEach(func() {
			// Clean up MySQL
			err := k8sClient.DeleteAllOf(ctx, &mysqlv1alpha1.MySQL{}, client.InNamespace(Namespace))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLName}, mysql)
			}).ShouldNot(Succeed())

			stopFunc()
			err = close()
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(100 * time.Millisecond)
		})

		When("Creating MySQLUser", func() {

			BeforeEach(func() {
				// Clean up MySQLUser
				err := k8sClient.DeleteAllOf(ctx, &mysqlv1alpha1.MySQLUser{}, client.InNamespace(Namespace))
				Expect(err).NotTo(HaveOccurred())

				// Clean up Secret
				err = k8sClient.DeleteAllOf(ctx, &v1.Secret{}, client.InNamespace(Namespace))
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				// Clean up MySQLUser
				err := k8sClient.DeleteAllOf(ctx, &mysqlv1alpha1.MySQLUser{}, client.InNamespace(Namespace))
				Expect(err).NotTo(HaveOccurred())

				// Clean up Secret
				err = k8sClient.DeleteAllOf(ctx, &v1.Secret{}, client.InNamespace(Namespace))
				Expect(err).NotTo(HaveOccurred())
			})

			It("Should not create Secret", func() {
				By("By creating a new MySQLUser")
				mysqlUser = newMySQLUser(APIVersion, Namespace, MySQLUserName, MySQLName)
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				// Secret should not be created
				secret := &v1.Secret{}
				Consistently(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: mysqlUser.Spec.PasswordSecretRef.Name}, secret)
					return errors.IsNotFound(err)
				}).Should(BeTrue())
			})

			It("Should have NotReady status with reason 'failed to connect to mysql'", func() {
				By("By creating a new MySQLUser")
				mysqlUser = newMySQLUser(APIVersion, Namespace, MySQLUserName, MySQLName)
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				// Secret should not be created
				secret := &v1.Secret{}
				Consistently(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: mysqlUser.Spec.PasswordSecretRef.Name}, secret)
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				// status.phase should be not ready
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Phase
				}).Should(Equal(constants.PhaseNotReady))

				// status.reason should be 'Failed to fetch MySQL client'
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Reason
				}).Should(Equal(constants.ReasonMySQLFetchFailed))
			})
		})

		When("Creating and deleting MySQLUser", func() {

			BeforeEach(func() {
				cleanUpMySQLUser(ctx, k8sClient, Namespace)
				cleanUpSecret(ctx, k8sClient, Namespace)
			})

			AfterEach(func() {
				cleanUpMySQLUser(ctx, k8sClient, Namespace)
				cleanUpSecret(ctx, k8sClient, Namespace)
			})

			It("Should delete MySQLUser", func() {
				By("By creating a new MySQLUser")
				mysqlUser = newMySQLUser(APIVersion, Namespace, MySQLUserName, MySQLName)
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				// Secret will not be created
				secret := &v1.Secret{}
				Consistently(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: mysqlUser.Spec.PasswordSecretRef.Name}, secret)
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				// Delete MySQLUser
				Expect(k8sClient.Delete(ctx, mysqlUser)).To(Succeed())
				Eventually(func() error {
					return k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
				}).Should(HaveOccurred())
			})
		})

		Context("With no MySQL found", func() {
			BeforeEach(func() {
				// Clean up MySQLUser
				cleanUpMySQLUser(ctx, k8sClient, Namespace)
				// Clean up MySQL
				cleanUpMySQL(ctx, k8sClient, Namespace)
			})
			It("Should have NotReady status with reason 'failed to fetch MySQL'", func() {
				By("By creating a new MySQLUser")
				mysqlUser = &mysqlv1alpha1.MySQLUser{
					TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "MySQLUser"},
					ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: MySQLUserName},
					Spec:       mysqlv1alpha1.MySQLUserSpec{ClusterName: MySQLName},
				}
				Expect(k8sClient.Create(ctx, mysqlUser)).Should(Succeed())

				// Secret will not be created
				secret := &v1.Secret{}
				Consistently(func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: mysqlUser.Spec.PasswordSecretRef.Name}, secret)
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				// status.phase should be not ready
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Phase
				}).Should(Equal(constants.PhaseNotReady))

				// status.reason should be 'Failed to fetch MySQL client'
				Eventually(func() string {
					err := k8sClient.Get(ctx, client.ObjectKey{Namespace: Namespace, Name: MySQLUserName}, mysqlUser)
					if err != nil {
						return ""
					}
					return mysqlUser.Status.Reason
				}).Should(Equal(constants.ReasonMySQLFetchFailed))
			})
		})
	})

	// Tests for calculateGrantDiff function
	Context("When calculating grant differences", func() {
		When("specific targets are removed", func() {
			It("should properly revoke privileges", func() {
				// Test scenario: User had privileges on multiple workload groups,
				// but some are removed in the updated grants
				oldGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'normal'",
					},
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'high'",
					},
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*",
					},
				}

				newGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'high'", // 'normal' workload group removed
					},
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*",
					},
				}

				grantsToRevoke, grantsToAdd := calculateGrantDiff(oldGrants, newGrants)

				// Should revoke USAGE_PRIV on 'normal' workload group
				Expect(grantsToRevoke).To(HaveLen(1))
				Expect(grantsToRevoke[0].Target).To(Equal("WORKLOAD GROUP 'normal'"))
				Expect(grantsToRevoke[0].Privileges).To(Equal([]string{"USAGE_PRIV"}))

				// No new grants should be added
				Expect(grantsToAdd).To(BeEmpty())
			})
		})

		When("privileges are changed", func() {
			It("should properly revoke only the removed privileges", func() {
				oldGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV", "ALTER_PRIV"},
						Target:     "*.*.*",
					},
				}

				newGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*", // ALTER_PRIV removed
					},
				}

				grantsToRevoke, grantsToAdd := calculateGrantDiff(oldGrants, newGrants)

				// Should revoke only ALTER_PRIV
				Expect(grantsToRevoke).To(HaveLen(1))
				Expect(grantsToRevoke[0].Target).To(Equal("*.*.*"))
				Expect(grantsToRevoke[0].Privileges).To(Equal([]string{"ALTER_PRIV"}))

				// No new grants should be added
				Expect(grantsToAdd).To(BeEmpty())
			})
		})

		When("entire target type is removed", func() {
			It("should properly revoke all privileges of that target type", func() {
				oldGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'normal'",
					},
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*",
					},
				}

				newGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*",
					},
					// No workload group privileges
				}

				grantsToRevoke, grantsToAdd := calculateGrantDiff(oldGrants, newGrants)

				// Should revoke USAGE_PRIV on 'normal' workload group
				Expect(grantsToRevoke).To(HaveLen(1))
				Expect(grantsToRevoke[0].Target).To(Equal("WORKLOAD GROUP 'normal'"))
				Expect(grantsToRevoke[0].Privileges).To(Equal([]string{"USAGE_PRIV"}))

				// No new grants should be added
				Expect(grantsToAdd).To(BeEmpty())
			})
		})

		When("new targets are added", func() {
			It("should correctly add the new grants", func() {
				oldGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV"},
						Target:     "*.*.*",
					},
				}

				newGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV"},
						Target:     "*.*.*",
					},
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'high'",
					},
				}

				grantsToRevoke, grantsToAdd := calculateGrantDiff(oldGrants, newGrants)

				// Nothing should be revoked
				Expect(grantsToRevoke).To(BeEmpty())

				// New workload group privilege should be added
				Expect(grantsToAdd).To(HaveLen(1))
				Expect(grantsToAdd[0].Target).To(Equal("WORKLOAD GROUP 'high'"))
				Expect(grantsToAdd[0].Privileges).To(Equal([]string{"USAGE_PRIV"}))
			})
		})

		When("both additions and revocations are needed", func() {
			It("should handle both operations correctly", func() {
				oldGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'normal'",
					},
					{
						Privileges: []string{"SELECT_PRIV"},
						Target:     "*.*.*",
					},
				}

				newGrants := []mysqlv1alpha1.Grant{
					{
						Privileges: []string{"SELECT_PRIV", "LOAD_PRIV"},
						Target:     "*.*.*", // Added LOAD_PRIV
					},
					{
						Privileges: []string{"USAGE_PRIV"},
						Target:     "WORKLOAD GROUP 'high'", // Changed from 'normal' to 'high'
					},
				}

				grantsToRevoke, grantsToAdd := calculateGrantDiff(oldGrants, newGrants)

				// Should revoke USAGE_PRIV on 'normal' workload group
				Expect(grantsToRevoke).To(HaveLen(1))
				Expect(grantsToRevoke[0].Target).To(Equal("WORKLOAD GROUP 'normal'"))
				Expect(grantsToRevoke[0].Privileges).To(Equal([]string{"USAGE_PRIV"}))

				// Should add LOAD_PRIV on *.*.* and USAGE_PRIV on 'high' workload group
				Expect(grantsToAdd).To(HaveLen(2))

				// Check if both expected grants are in grantsToAdd
				foundTable := false
				foundWorkload := false
				for _, grant := range grantsToAdd {
					if grant.Target == "*.*.*" {
						Expect(grant.Privileges).To(Equal([]string{"LOAD_PRIV"}))
						foundTable = true
					} else if grant.Target == "WORKLOAD GROUP 'high'" {
						Expect(grant.Privileges).To(Equal([]string{"USAGE_PRIV"}))
						foundWorkload = true
					}
				}
				Expect(foundTable && foundWorkload).To(BeTrue(), "Expected both table and workload group grants")
			})
		})
	})
})
