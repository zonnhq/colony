// Package client is the node's gRPC connection to the Coordinator. It is a thin
// wrapper around the generated NodeService client. Reconnect and backoff live in
// the daemon, this package just dials and carries calls.
package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/projectcolony/colony/node-gui/internal/config"
	"github.com/projectcolony/colony/node-gui/internal/resources"

	colonyv1 "github.com/projectcolony/colony/coordinator/proto"
)

// Client holds a live connection to the Coordinator.
type Client struct {
	conn *grpc.ClientConn
	svc  colonyv1.NodeServiceClient
}

// Dial opens a connection to the Coordinator gRPC address.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, svc: colonyv1.NewNodeServiceClient(conn)}, nil
}

// Close tears down the connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// RegisterResult is the outcome of a successful registration.
type RegisterResult struct {
	NodeID            string
	HeartbeatInterval time.Duration
	Colonies          []Colony
}

// Colony is a summary returned at registration.
type Colony struct {
	ID   string
	Name string
}

// Register onboards this node and returns the assigned id. The fingerprint lets
// the Coordinator dedupe the same physical machine.
func (c *Client) Register(ctx context.Context, cfg config.Config, specs resources.Specs, fingerprint string) (RegisterResult, error) {
	resp, err := c.svc.Register(ctx, &colonyv1.RegisterRequest{
		NodeId:      cfg.NodeID,
		Name:        cfg.NodeName,
		Os:          specs.OS,
		Arch:        specs.Arch,
		Fingerprint: fingerprint,
		Resources: &colonyv1.Resources{
			CpuCores:    int32(specs.CPUCores),
			RamGb:       int32(specs.RAMGB),
			GpuModel:    specs.GPUModel,
			GpuMemoryGb: int32(specs.GPUMemoryGB),
			DiskGb:      int32(specs.DiskGB),
		},
		Allocated: &colonyv1.Allocation{
			CpuCores:      int32(cfg.Allocation.CPUCores),
			RamGb:         int32(cfg.Allocation.RAMGB),
			GpuMemoryGb:   int32(cfg.Allocation.GPUMemory),
			BandwidthMbps: int32(cfg.Allocation.BandwidthMbps),
		},
	})
	if err != nil {
		return RegisterResult{}, err
	}

	interval := time.Duration(resp.GetHeartbeatIntervalSeconds()) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	colonies := make([]Colony, 0, len(resp.GetColonies()))
	for _, c := range resp.GetColonies() {
		colonies = append(colonies, Colony{ID: c.GetId(), Name: c.GetName()})
	}
	return RegisterResult{
		NodeID:            resp.GetNodeId(),
		HeartbeatInterval: interval,
		Colonies:          colonies,
	}, nil
}

// HeartbeatStream opens the bidirectional heartbeat stream.
func (c *Client) HeartbeatStream(ctx context.Context) (colonyv1.NodeService_HeartbeatClient, error) {
	return c.svc.Heartbeat(ctx)
}

// SendHeartbeat pushes one utilization sample on an open stream and returns the
// Coordinator's acknowledgement.
func SendHeartbeat(stream colonyv1.NodeService_HeartbeatClient, nodeID string, u resources.Utilization, workerCPUCores, workerRAMGB float64, workerActive bool) (*colonyv1.HeartbeatResponse, error) {
	err := stream.Send(&colonyv1.HeartbeatRequest{
		NodeId:    nodeID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Utilization: &colonyv1.Utilization{
			CpuUsed:      u.CPUUsedCores,
			RamUsedGb:    u.RAMUsedGB,
			GpuMemUsedGb: u.GPUMemUsedGB,
			GpuTempC:     u.GPUTempC,
		},
		WorkerCpuCores: workerCPUCores,
		WorkerRamGb:    workerRAMGB,
		WorkerActive:   workerActive,
	})
	if err != nil {
		return nil, err
	}
	return stream.Recv()
}

// ReportError pushes a node side error into the Coordinator issues feed.
func (c *Client) ReportError(ctx context.Context, nodeID, level, message string) error {
	_, err := c.svc.ReportError(ctx, &colonyv1.ReportErrorRequest{
		NodeId:    nodeID,
		Level:     level,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	return err
}

// SetOffline tells the Coordinator this node is going offline on purpose, so the
// fleet reflects it immediately rather than waiting for the reaper.
func (c *Client) SetOffline(ctx context.Context, nodeID, reason string) error {
	_, err := c.svc.SetOffline(ctx, &colonyv1.SetOfflineRequest{
		NodeId: nodeID,
		Reason: reason,
	})
	return err
}
