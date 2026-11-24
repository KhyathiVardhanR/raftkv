package server

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics implements raft.Observer and exposes node-level Prometheus metrics.
type metrics struct {
	reg *prometheus.Registry

	term, isLeader, commitIdx, appliedIdx, logLen  *prometheus.GaugeVec
	elections, leaderChanges                       *prometheus.CounterVec
	snapshotsSent, snapshotsInstalled, compactions *prometheus.CounterVec
	commitLatency                                  *prometheus.HistogramVec
	replicationRTT                                 *prometheus.HistogramVec
	walSync                                        prometheus.Histogram
	deviceFlush                                    prometheus.Histogram
	kvRequests                                     *prometheus.CounterVec
	kvLatency                                      *prometheus.HistogramVec
	lsmSize                                        prometheus.GaugeFunc
}

var latencyBuckets = []float64{
	.0005, .001, .002, .003, .005, .0075, .01, .015, .02, .03, .05, .075, .1, .25, .5, 1, 2.5,
}

func newMetrics(n *Node) *metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	shardLbl := []string{"shard"}
	m := &metrics{
		reg:                reg,
		term:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_raft_term", Help: "Current Raft term."}, shardLbl),
		isLeader:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_raft_is_leader", Help: "1 if this node leads the shard."}, shardLbl),
		commitIdx:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_raft_commit_index", Help: "Raft commit index."}, shardLbl),
		appliedIdx:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_raft_applied_index", Help: "Last applied index."}, shardLbl),
		logLen:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_raft_log_entries", Help: "Entries retained in the Raft log."}, shardLbl),
		elections:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_raft_elections_total", Help: "Elections started."}, shardLbl),
		leaderChanges:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_raft_leader_elected_total", Help: "Times this node became leader."}, shardLbl),
		snapshotsSent:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_raft_snapshots_sent_total", Help: "InstallSnapshot RPCs sent."}, shardLbl),
		snapshotsInstalled: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_raft_snapshots_installed_total", Help: "Snapshots installed."}, shardLbl),
		compactions:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_raft_log_compactions_total", Help: "Log compactions."}, shardLbl),
		commitLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_raft_commit_latency_seconds", Help: "Proposal to quorum commit + apply (replication latency).", Buckets: latencyBuckets,
		}, shardLbl),
		replicationRTT: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_raft_append_entries_rtt_seconds", Help: "AppendEntries round trip incl. follower fsync.", Buckets: latencyBuckets,
		}, []string{"peer"}),
		walSync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_wal_fsync_seconds", Help: "Raft WAL fsync duration.", Buckets: latencyBuckets,
		}),
		deviceFlush: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_wal_group_flush_seconds", Help: "Shared device cache flush (group commit across shards).", Buckets: latencyBuckets,
		}),
		kvRequests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_kv_requests_total", Help: "Client requests by op and result."}, []string{"op", "code"}),
		kvLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_kv_request_duration_seconds", Help: "Server-side client request latency.", Buckets: latencyBuckets,
		}, []string{"op"}),
	}
	m.lsmSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "raftkv_lsm_disk_bytes", Help: "LSM engine on-disk size."},
		func() float64 { return float64(n.engine.Metrics().DiskSpaceUsage()) })
	reg.MustRegister(m.term, m.isLeader, m.commitIdx, m.appliedIdx, m.logLen, m.elections, m.leaderChanges,
		m.snapshotsSent, m.snapshotsInstalled, m.compactions, m.commitLatency, m.replicationRTT, m.walSync, m.deviceFlush,
		m.kvRequests, m.kvLatency, m.lsmSize)
	return m
}

func sl(g uint32) string { return strconv.Itoa(int(g)) }

func (m *metrics) ElectionStarted(g uint32) { m.elections.WithLabelValues(sl(g)).Inc() }
func (m *metrics) BecameLeader(g uint32)    { m.leaderChanges.WithLabelValues(sl(g)).Inc() }
func (m *metrics) CommitLatency(g uint32, d time.Duration) {
	m.commitLatency.WithLabelValues(sl(g)).Observe(d.Seconds())
}
func (m *metrics) ReplicationRTT(g uint32, peer string, d time.Duration) {
	m.replicationRTT.WithLabelValues(peer).Observe(d.Seconds())
}
func (m *metrics) SnapshotSent(g uint32)      { m.snapshotsSent.WithLabelValues(sl(g)).Inc() }
func (m *metrics) SnapshotInstalled(g uint32) { m.snapshotsInstalled.WithLabelValues(sl(g)).Inc() }
func (m *metrics) LogCompacted(g uint32)      { m.compactions.WithLabelValues(sl(g)).Inc() }
func (m *metrics) WALSync(d time.Duration)    { m.walSync.Observe(d.Seconds()) }

// refresh copies Raft status into gauges.
func (m *metrics) refresh(n *Node) {
	for g, r := range n.groups {
		st := r.Status()
		l := sl(g)
		m.term.WithLabelValues(l).Set(float64(st.Term))
		lead := 0.0
		if st.Role == "leader" {
			lead = 1
		}
		m.isLeader.WithLabelValues(l).Set(lead)
		m.commitIdx.WithLabelValues(l).Set(float64(st.CommitIndex))
		m.appliedIdx.WithLabelValues(l).Set(float64(st.LastApplied))
		m.logLen.WithLabelValues(l).Set(float64(st.LastIndex - st.SnapIndex))
	}
}
