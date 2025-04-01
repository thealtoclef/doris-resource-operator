package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const MetricsNamespace = "mysqloperator"

type MysqlUserTotalAdaptor struct {
	metric prometheus.Counter
}

// StorageVaultTotalAdaptor is a wrapper for prometheus Counter metrics
type StorageVaultTotalAdaptor struct {
	metric prometheus.Counter
}

func (m MysqlUserTotalAdaptor) Increment() {
	m.metric.Inc()
}

func (m StorageVaultTotalAdaptor) Increment() {
	m.metric.Inc()
}

var (
	userCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "mysql_user_created_total",
			Help:      "Number of created MySQL User",
		},
	)

	mysqlUserDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "mysql_user_deleted_total",
			Help:      "Number of deleted MySQL User",
		},
	)

	storageVaultCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "storage_vault_created_total",
			Help:      "Number of created Storage Vaults",
		},
	)

	storageVaultDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "storage_vault_deleted_total",
			Help:      "Number of deleted Storage Vaults",
		},
	)

	MysqlUserCreatedTotal *MysqlUserTotalAdaptor = &MysqlUserTotalAdaptor{metric: userCreatedTotal}
	MysqlUserDeletedTotal *MysqlUserTotalAdaptor = &MysqlUserTotalAdaptor{metric: mysqlUserDeletedTotal}

	StorageVaultCreatedTotal *StorageVaultTotalAdaptor = &StorageVaultTotalAdaptor{metric: storageVaultCreatedTotal}
	StorageVaultDeletedTotal *StorageVaultTotalAdaptor = &StorageVaultTotalAdaptor{metric: storageVaultDeletedTotal}
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(
		userCreatedTotal,
		mysqlUserDeletedTotal,
		storageVaultCreatedTotal,
		storageVaultDeletedTotal,
	)
}
