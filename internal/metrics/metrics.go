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

// CatalogTotalAdaptor is a wrapper for prometheus Counter metrics
type CatalogTotalAdaptor struct {
	metric prometheus.Counter
}

// WorkloadGroupTotalAdaptor is a wrapper for prometheus Counter metrics
type WorkloadGroupTotalAdaptor struct {
	metric prometheus.Counter
}

func (m MysqlUserTotalAdaptor) Increment() {
	m.metric.Inc()
}

func (m StorageVaultTotalAdaptor) Increment() {
	m.metric.Inc()
}

func (m CatalogTotalAdaptor) Increment() {
	m.metric.Inc()
}

func (m WorkloadGroupTotalAdaptor) Increment() {
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

	catalogCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "catalog_created_total",
			Help:      "Number of created Catalogs",
		},
	)

	catalogDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "catalog_deleted_total",
			Help:      "Number of deleted Catalogs",
		},
	)

	workloadGroupCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "workload_group_created_total",
			Help:      "Number of created Workload Groups",
		},
	)

	workloadGroupDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "workload_group_deleted_total",
			Help:      "Number of deleted Workload Groups",
		},
	)

	MysqlUserCreatedTotal *MysqlUserTotalAdaptor = &MysqlUserTotalAdaptor{metric: userCreatedTotal}
	MysqlUserDeletedTotal *MysqlUserTotalAdaptor = &MysqlUserTotalAdaptor{metric: mysqlUserDeletedTotal}

	StorageVaultCreatedTotal *StorageVaultTotalAdaptor = &StorageVaultTotalAdaptor{metric: storageVaultCreatedTotal}
	StorageVaultDeletedTotal *StorageVaultTotalAdaptor = &StorageVaultTotalAdaptor{metric: storageVaultDeletedTotal}

	CatalogCreatedTotal *CatalogTotalAdaptor = &CatalogTotalAdaptor{metric: catalogCreatedTotal}
	CatalogDeletedTotal *CatalogTotalAdaptor = &CatalogTotalAdaptor{metric: catalogDeletedTotal}

	WorkloadGroupCreatedTotal *WorkloadGroupTotalAdaptor = &WorkloadGroupTotalAdaptor{metric: workloadGroupCreatedTotal}
	WorkloadGroupDeletedTotal *WorkloadGroupTotalAdaptor = &WorkloadGroupTotalAdaptor{metric: workloadGroupDeletedTotal}
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(
		userCreatedTotal,
		mysqlUserDeletedTotal,
		storageVaultCreatedTotal,
		storageVaultDeletedTotal,
		catalogCreatedTotal,
		catalogDeletedTotal,
		workloadGroupCreatedTotal,
		workloadGroupDeletedTotal,
	)
}
