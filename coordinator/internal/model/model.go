// Package model holds the shared domain types for the Coordinator. Field names
// and JSON tags mirror blueprint_v2.md section 2.6 so the gRPC messages, the
// SQLite rows, and the REST responses all describe the same shapes.
package model

import "time"

// Node status values.
const (
	StatusOnline  = "ONLINE"
	StatusBusy    = "BUSY"
	StatusOffline = "OFFLINE"
)

// Error levels used in the issues feed.
const (
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
)

// Colony status values.
const (
	ColonyActive  = "ACTIVE"
	ColonyDeleted = "DELETED"
)

// Resources are the total physical resources on a machine.
type Resources struct {
	CPUCores  int    `json:"cpu_cores"`
	RAMGB     int    `json:"ram_gb"`
	GPUModel  string `json:"gpu_model"`
	GPUMemory int    `json:"gpu_memory_gb"`
	DiskGB    int    `json:"disk_gb"`
}

// Allocation is the share of resources a contributor donates.
type Allocation struct {
	CPUCores      int `json:"cpu_cores"`
	RAMGB         int `json:"ram_gb"`
	GPUMemory     int `json:"gpu_memory_gb"`
	BandwidthMbps int `json:"bandwidth_mbps"`
}

// Utilization is the live usage reported in a heartbeat.
type Utilization struct {
	CPUUsed      float64 `json:"cpu_used"`
	RAMUsedGB    float64 `json:"ram_used_gb"`
	GPUMemUsedGB float64 `json:"gpu_mem_used_gb"`
	GPUTempC     float64 `json:"gpu_temp_c"`
}

// Node is a registered machine and its last known state.
type Node struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	OS          string      `json:"os"`
	Arch        string      `json:"arch"`
	Resources   Resources   `json:"resources"`
	Allocated   Allocation  `json:"allocated"`
	Status      string      `json:"status"`
	ColonyID    string      `json:"colony_id"`
	LastSeen    *time.Time  `json:"last_seen"`
	CreatedAt   time.Time   `json:"created_at"`
	Utilization Utilization `json:"utilization"`
	Score       float64     `json:"contribution_score"`
	// ComputeUnits is the normalized single number that folds this node's donated
	// CPU, RAM, and GPU into one comparable metric, so a CPU heavy machine and a
	// GPU heavy machine add to the same colony pool. Unlike Score it is not zeroed
	// when offline, so the UI can still show what the node pledges.
	ComputeUnits float64 `json:"compute_units"`
	// Fingerprint is a stable hardware identity used only to dedupe the same
	// physical machine. It is never exposed over the REST API.
	Fingerprint string `json:"-"`
}

// Compute unit weights. One CPU core is the base unit; RAM is lighter per GB and
// GPU memory is heavier, reflecting its scarcity and value for inference. These
// are the single normalization for a heterogeneous colony (see blueprint_v2 2.2).
const (
	cpuUnitWeight = 1.0
	ramUnitWeight = 0.5
	gpuUnitWeight = 1.5
)

// WeightedCapacity folds the donated resources into normalized compute units.
func (n Node) WeightedCapacity() float64 {
	return float64(n.Allocated.CPUCores)*cpuUnitWeight +
		float64(n.Allocated.RAMGB)*ramUnitWeight +
		float64(n.Allocated.GPUMemory)*gpuUnitWeight
}

// ContributionScore is the capacity that counts toward the leaderboard. Offline
// nodes score zero so the ranking rewards being available.
func (n Node) ContributionScore() float64 {
	if n.Status == StatusOffline {
		return 0
	}
	return n.WeightedCapacity()
}

// Colony is a logical group of nodes that behaves as one supercomputer.
type Colony struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	NodeIDs   []string  `json:"node_ids"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// NodeError is a single entry in the issues feed.
type NodeError struct {
	ID      int64     `json:"id"`
	NodeID  string    `json:"node_id"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
	TS      time.Time `json:"ts"`
}

// Event categories for the full activity feed.
const (
	CategoryNode   = "node"
	CategoryColony = "colony"
	CategoryJob    = "job"
	CategorySystem = "system"
)

// Event is a single entry in the full activity log shown on the admin dashboard.
type Event struct {
	ID       int64     `json:"id"`
	TS       time.Time `json:"ts"`
	Level    string    `json:"level"`
	Category string    `json:"category"`
	NodeID   string    `json:"node_id"`
	NodeName string    `json:"node_name"`
	Message  string    `json:"message"`
}

// Feedback is a single submission from the admin dashboard's feedback form.
type Feedback struct {
	ID      int64     `json:"id"`
	TS      time.Time `json:"ts"`
	Message string    `json:"message"`
	Email   string    `json:"email"`
}

// Stats are the fleet totals shown on the admin overview.
type Stats struct {
	TotalNodes    int `json:"total_nodes"`
	OnlineNodes   int `json:"online_nodes"`
	OfflineNodes  int `json:"offline_nodes"`
	BusyNodes     int `json:"busy_nodes"`
	TotalCPUCores int `json:"total_cpu_cores"`
	TotalRAMGB    int `json:"total_ram_gb"`
	TotalGPUs     int `json:"total_gpus"`
	// Composition of the online fleet: how the single compute pool is made up of
	// CPU heavy and GPU bearing machines, and the donated GPU memory behind it.
	TotalComputeUnits float64 `json:"total_compute_units"`
	TotalGPUMemoryGB  int     `json:"total_gpu_memory_gb"`
	GPUNodes          int     `json:"gpu_nodes"`
	CPUOnlyNodes      int     `json:"cpu_only_nodes"`
	ActiveColonies    int     `json:"active_colonies"`
}

// Contribution is the durable work record accumulated from node heartbeats.
// Resource-hours measure actual Zonn worker process usage, not whole-machine use.
type Contribution struct {
	NodeID          string    `json:"node_id"`
	NodeName        string    `json:"node_name"`
	ColonyID        string    `json:"colony_id"`
	OnlineHours     float64   `json:"online_hours"`
	ActiveHours     float64   `json:"active_hours"`
	CPUCoreHours    float64   `json:"cpu_core_hours"`
	RAMGBHours      float64   `json:"ram_gb_hours"`
	CurrentCPUCores float64   `json:"current_cpu_cores"`
	CurrentRAMGB    float64   `json:"current_ram_gb"`
	Active          bool      `json:"active"`
	Samples         int64     `json:"samples"`
	UpdatedAt       time.Time `json:"updated_at"`
}
