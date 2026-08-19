// Package grpcserver implements the NodeService that node agents talk to.
package grpcserver

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/projectcolony/colony/coordinator/internal/db"
	"github.com/projectcolony/colony/coordinator/internal/model"
	"github.com/projectcolony/colony/coordinator/internal/orchestrator"
	"github.com/projectcolony/colony/coordinator/internal/state"
	colonyv1 "github.com/projectcolony/colony/coordinator/proto"
)

// Server implements colonyv1.NodeServiceServer.
type Server struct {
	colonyv1.UnimplementedNodeServiceServer
	store             *db.DB
	cache             *state.Cache
	orch              *orchestrator.Manager
	heartbeatInterval time.Duration
}

// New builds a gRPC server handler.
func New(store *db.DB, cache *state.Cache, orch *orchestrator.Manager, heartbeatInterval time.Duration) *Server {
	return &Server{store: store, cache: cache, orch: orch, heartbeatInterval: heartbeatInterval}
}

// Register onboards a node, assigns an id, and returns the active colonies.
func (s *Server) Register(ctx context.Context, req *colonyv1.RegisterRequest) (*colonyv1.RegisterResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "node name is required")
	}

	// Resolve the canonical id. A matching hardware fingerprint wins so the same
	// physical machine collapses to one record even if it lost its stored id or
	// presents a different one. Otherwise reuse the node's own stable id, and only
	// mint a fresh id when there is nothing to match.
	nodeID := strings.TrimSpace(req.GetNodeId())
	fingerprint := strings.TrimSpace(req.GetFingerprint())
	if fpID, err := s.store.FindNodeIDByFingerprint(fingerprint); err != nil {
		log.Printf("register: fingerprint lookup for %q: %v", name, err)
	} else if fpID != "" {
		nodeID = fpID
	}
	if nodeID == "" {
		nodeID = uuid.NewString()
	}

	now := time.Now().UTC()
	node := model.Node{
		ID:          nodeID,
		Name:        name,
		OS:          req.GetOs(),
		Arch:        req.GetArch(),
		Resources:   resourcesFromProto(req.GetResources()),
		Allocated:   allocationFromProto(req.GetAllocated()),
		Status:      model.StatusOnline,
		LastSeen:    &now,
		CreatedAt:   now,
		Fingerprint: fingerprint,
	}

	existed, err := s.store.NodeExists(node.ID)
	if err != nil {
		log.Printf("register: check existing node %q: %v", name, err)
	}
	// Was the node previously offline (or unknown)? Used to log a genuine online
	// transition without spamming on every settings driven reconnect.
	prev, known := s.cache.Get(node.ID)
	wasOffline := !known || prev.Status == model.StatusOffline
	// Keep any existing colony membership across a reconnect. UpsertNode leaves
	// colony_id untouched in the database, so mirror that in the cache.
	node.ColonyID = s.cache.ColonyOf(node.ID)
	if err := s.store.UpsertNode(node); err != nil {
		log.Printf("register: upsert node %q: %v", name, err)
		return nil, status.Error(codes.Internal, "could not register node")
	}
	s.cache.Upsert(node)
	if !existed {
		// Only announce genuinely new nodes so reconnects do not spam the feed.
		if err := s.store.InsertError(node.ID, model.LevelInfo, "Node registered with the Coordinator", now); err != nil {
			log.Printf("register: record event for %q: %v", name, err)
		}
		s.logEvent(model.Event{TS: now, Level: model.LevelInfo, Category: model.CategoryNode,
			NodeID: node.ID, NodeName: node.Name, Message: "Node " + node.Name + " registered with the Colony"})
	} else if wasOffline {
		s.logEvent(model.Event{TS: now, Level: model.LevelInfo, Category: model.CategoryNode,
			NodeID: node.ID, NodeName: node.Name, Message: "Node " + node.Name + " came online"})
	}
	log.Printf("register: %s (%s) id=%s new=%v", node.Name, node.OS, node.ID, !existed)

	colonies, err := s.store.ListColonies()
	if err != nil {
		log.Printf("register: list colonies: %v", err)
	}
	summaries := make([]*colonyv1.ColonySummary, 0, len(colonies))
	for _, c := range colonies {
		summaries = append(summaries, &colonyv1.ColonySummary{Id: c.ID, Name: c.Name})
	}

	return &colonyv1.RegisterResponse{
		NodeId:                   node.ID,
		Colonies:                 summaries,
		HeartbeatIntervalSeconds: int32(s.heartbeatInterval.Seconds()),
	}, nil
}

// Heartbeat consumes the node's utilization stream and replies with the node's
// current colony assignment on every beat.
func (s *Server) Heartbeat(stream colonyv1.NodeService_HeartbeatServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		nodeID := req.GetNodeId()
		ts := parseTimestamp(req.GetTimestamp())
		util := utilizationFromProto(req.GetUtilization())

		if ok := s.cache.Touch(nodeID, util, ts); !ok {
			// Unknown node, ask it to register again rather than dropping the stream.
			if sendErr := stream.Send(&colonyv1.HeartbeatResponse{Status: "REREGISTER"}); sendErr != nil {
				return sendErr
			}
			continue
		}

		colonyID := s.cache.ColonyOf(nodeID)
		if err := s.store.RecordHeartbeat(nodeID, util, colonyID, ts, req.GetWorkerCpuCores(), req.GetWorkerRamGb(), req.GetWorkerActive()); err != nil {
			log.Printf("heartbeat: record for %s: %v", nodeID, err)
		}

		r := s.cache.RankOf(nodeID)
		command := ""
		if s.orch != nil {
			command = s.orch.TakeCommand(nodeID)
		}
		if err := stream.Send(&colonyv1.HeartbeatResponse{
			Status:            "ACK",
			AssignedColonyId:  colonyID,
			Command:           command,
			Rank:              int32(r.Rank),
			ActiveNodes:       int32(r.ActiveNodes),
			ContributionScore: r.Score,
			AverageScore:      r.AverageScore,
		}); err != nil {
			return err
		}
	}
}

// logEvent appends to the full activity log, best effort.
func (s *Server) logEvent(e model.Event) {
	if err := s.store.InsertEvent(e); err != nil {
		log.Printf("event: %v", err)
	}
}

// SetOffline marks a node offline at once when it reports a deliberate pause, so
// the fleet reflects it immediately instead of waiting for the reaper.
func (s *Server) SetOffline(ctx context.Context, req *colonyv1.SetOfflineRequest) (*colonyv1.SetOfflineResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	now := time.Now().UTC()
	if n, changed := s.cache.SetOffline(nodeID); changed {
		if err := s.store.SetNodeStatus(nodeID, model.StatusOffline); err != nil {
			log.Printf("setoffline: db status for %s: %v", nodeID, err)
		}
		reason := strings.TrimSpace(req.GetReason())
		msg := "Node " + n.Name + " went offline"
		if reason != "" {
			msg = "Node " + n.Name + " went offline (" + reason + ")"
		}
		s.logEvent(model.Event{TS: now, Level: model.LevelInfo, Category: model.CategoryNode,
			NodeID: nodeID, NodeName: n.Name, Message: msg})
		log.Printf("setoffline: %s", msg)
	}
	return &colonyv1.SetOfflineResponse{Ok: true}, nil
}

// ReportError pushes a node side error into the issues feed.
func (s *Server) ReportError(ctx context.Context, req *colonyv1.ReportErrorRequest) (*colonyv1.ReportErrorResponse, error) {
	level := strings.ToUpper(strings.TrimSpace(req.GetLevel()))
	if level == "" {
		level = model.LevelError
	}
	ts := parseTimestamp(req.GetTimestamp())
	if err := s.store.InsertError(req.GetNodeId(), level, req.GetMessage(), ts); err != nil {
		log.Printf("report error: insert for %s: %v", req.GetNodeId(), err)
		return nil, status.Error(codes.Internal, "could not record error")
	}
	nodeName := req.GetNodeId()
	if n, ok := s.cache.Get(req.GetNodeId()); ok {
		nodeName = n.Name
	}
	s.logEvent(model.Event{TS: ts, Level: level, Category: model.CategoryNode,
		NodeID: req.GetNodeId(), NodeName: nodeName, Message: req.GetMessage()})
	if s.orch != nil {
		s.orch.FailJobFromNode(req.GetNodeId(), req.GetMessage())
	}
	return &colonyv1.ReportErrorResponse{Ok: true}, nil
}

func resourcesFromProto(r *colonyv1.Resources) model.Resources {
	if r == nil {
		return model.Resources{}
	}
	return model.Resources{
		CPUCores:  int(r.GetCpuCores()),
		RAMGB:     int(r.GetRamGb()),
		GPUModel:  r.GetGpuModel(),
		GPUMemory: int(r.GetGpuMemoryGb()),
		DiskGB:    int(r.GetDiskGb()),
	}
}

func allocationFromProto(a *colonyv1.Allocation) model.Allocation {
	if a == nil {
		return model.Allocation{}
	}
	return model.Allocation{
		CPUCores:      int(a.GetCpuCores()),
		RAMGB:         int(a.GetRamGb()),
		GPUMemory:     int(a.GetGpuMemoryGb()),
		BandwidthMbps: int(a.GetBandwidthMbps()),
	}
}

func utilizationFromProto(u *colonyv1.Utilization) model.Utilization {
	if u == nil {
		return model.Utilization{}
	}
	return model.Utilization{
		CPUUsed:      u.GetCpuUsed(),
		RAMUsedGB:    u.GetRamUsedGb(),
		GPUMemUsedGB: u.GetGpuMemUsedGb(),
		GPUTempC:     u.GetGpuTempC(),
	}
}

func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Now().UTC()
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}
