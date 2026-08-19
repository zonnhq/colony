// Package rest serves the admin dashboard API over HTTP.
package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"

	"github.com/projectcolony/colony/coordinator/internal/db"
	"github.com/projectcolony/colony/coordinator/internal/model"
	"github.com/projectcolony/colony/coordinator/internal/orchestrator"
	"github.com/projectcolony/colony/coordinator/internal/state"
)

// Server holds the dependencies for the HTTP handlers.
type Server struct {
	store *db.DB
	cache *state.Cache
	orch  *orchestrator.Manager
}

// New builds a REST server.
func New(store *db.DB, cache *state.Cache, orch *orchestrator.Manager) *Server {
	return &Server{store: store, cache: cache, orch: orch}
}

// Router returns the configured HTTP handler.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Content-Type"},
	}))

	r.Get("/healthz", s.handleHealth)

	// The relay is a raw WebSocket upgrade used during the LLM test.
	if s.orch != nil {
		r.Get("/relay", s.orch.ServeRelay)
	}

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/nodes", s.handleListNodes)
		r.Get("/nodes/{id}", s.handleGetNode)
		r.Get("/stats", s.handleStats)
		r.Get("/leaderboard", s.handleLeaderboard)
		r.Get("/contributions", s.handleContributions)
		r.Get("/colonies", s.handleListColonies)
		r.Post("/colonies", s.handleCreateColony)
		r.Delete("/colonies/{id}", s.handleDeleteColony)
		r.Post("/colonies/{id}/deploy-llm", s.handleDeployLLM)
		r.Get("/errors", s.handleListErrors)
		r.Get("/feedback", s.handleListFeedback)
		r.Post("/feedback", s.handleCreateFeedback)
		r.Get("/activity", s.handleListActivity)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
	})

	return r
}

func (s *Server) handleContributions(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListContributions(strings.TrimSpace(r.URL.Query().Get("colony_id")))
	if err != nil {
		log.Printf("rest: list contributions: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list contributions")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.cache.List())
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	node, ok := s.cache.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "node not found")
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var stats model.Stats
	for _, n := range s.cache.List() {
		stats.TotalNodes++
		switch n.Status {
		case model.StatusOnline:
			stats.OnlineNodes++
		case model.StatusBusy:
			stats.BusyNodes++
		default:
			stats.OfflineNodes++
		}
		// Available compute counts only nodes that are currently reachable. The
		// composition splits that one pool into its CPU and GPU contributions so
		// the operator sees how a heterogeneous fleet adds up.
		if n.Status == model.StatusOnline || n.Status == model.StatusBusy {
			stats.TotalCPUCores += n.Allocated.CPUCores
			stats.TotalRAMGB += n.Allocated.RAMGB
			stats.TotalComputeUnits += n.WeightedCapacity()
			if strings.TrimSpace(n.Resources.GPUModel) != "" {
				stats.TotalGPUs++
			}
			if n.Allocated.GPUMemory > 0 {
				stats.GPUNodes++
				stats.TotalGPUMemoryGB += n.Allocated.GPUMemory
			} else {
				stats.CPUOnlyNodes++
			}
		}
	}
	if colonies, err := s.store.ListColonies(); err == nil {
		stats.ActiveColonies = len(colonies)
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	nodes := s.cache.List()
	sort.SliceStable(nodes, func(i, j int) bool {
		return nodes[i].Score > nodes[j].Score
	})
	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleListColonies(w http.ResponseWriter, r *http.Request) {
	colonies, err := s.store.ListColonies()
	if err != nil {
		log.Printf("rest: list colonies: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list colonies")
		return
	}
	writeJSON(w, http.StatusOK, colonies)
}

type createColonyRequest struct {
	Name    string   `json:"name"`
	NodeIDs []string `json:"node_ids"`
}

func (s *Server) handleCreateColony(w http.ResponseWriter, r *http.Request) {
	var req createColonyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "colony name is required")
		return
	}
	if len(req.NodeIDs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one node is required")
		return
	}
	for _, id := range req.NodeIDs {
		if _, ok := s.cache.Get(id); !ok {
			writeError(w, http.StatusBadRequest, "unknown node id: "+id)
			return
		}
	}

	colony := model.Colony{
		ID:        uuid.NewString(),
		Name:      req.Name,
		NodeIDs:   req.NodeIDs,
		Status:    model.ColonyActive,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.store.CreateColony(colony); err != nil {
		log.Printf("rest: create colony: %v", err)
		writeError(w, http.StatusInternalServerError, "could not create colony")
		return
	}
	s.cache.AssignColony(req.NodeIDs, colony.ID)
	log.Printf("rest: created colony %q (%s) with %d nodes", colony.Name, colony.ID, len(req.NodeIDs))
	s.logEvent(model.Event{TS: time.Now().UTC(), Level: model.LevelInfo, Category: model.CategoryColony,
		Message: fmt.Sprintf("Colony %q created with %d node(s)", colony.Name, len(req.NodeIDs))})
	writeJSON(w, http.StatusCreated, colony)
}

// logEvent appends to the full activity log, best effort.
func (s *Server) logEvent(e model.Event) {
	if err := s.store.InsertEvent(e); err != nil {
		log.Printf("rest: event: %v", err)
	}
}

func (s *Server) handleListActivity(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := s.store.ListEvents(limit)
	if err != nil {
		log.Printf("rest: list activity: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list activity")
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleDeleteColony(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ok, err := s.store.ColonyExists(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not check colony")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "colony not found")
		return
	}
	if err := s.store.DeleteColony(id); err != nil {
		log.Printf("rest: delete colony: %v", err)
		writeError(w, http.StatusInternalServerError, "could not delete colony")
		return
	}
	s.cache.ReleaseColony(id)
	log.Printf("rest: deleted colony %s", id)
	s.logEvent(model.Event{TS: time.Now().UTC(), Level: model.LevelInfo, Category: model.CategoryColony,
		Message: "Colony deleted and its nodes released to idle"})
	w.WriteHeader(http.StatusNoContent)
}

type deployLLMRequest struct {
	Prompt       string `json:"prompt"`
	Model        string `json:"model"`
	Engine       string `json:"engine"`
	MaxNewTokens int    `json:"max_new_tokens"`
}

func (s *Server) handleDeployLLM(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil {
		writeError(w, http.StatusServiceUnavailable, "orchestrator is not available")
		return
	}
	colonyID := chi.URLParam(r, "id")

	var req deployLLMRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Prompt = strings.TrimSpace(req.Prompt)
	if req.Prompt == "" {
		writeError(w, http.StatusBadRequest, "a prompt is required")
		return
	}

	// Pick the first two reachable nodes in the colony. A one-node Zone runs
	// both roles locally so the full split and relay pipeline remains testable.
	var members []model.Node
	for _, n := range s.cache.List() {
		if n.ColonyID == colonyID && (n.Status == model.StatusOnline || n.Status == model.StatusBusy) {
			members = append(members, n)
		}
	}
	if len(members) < 1 {
		writeError(w, http.StatusBadRequest, "the colony needs at least one online node for the inference test")
		return
	}
	// Give the more capable member the primary role. The primary repeatedly runs
	// the lower model layers and owns token history, so assigning it by database
	// insertion order can put the heaviest work on the smallest remote PC.
	sort.SliceStable(members, func(i, j int) bool {
		return members[i].WeightedCapacity() > members[j].WeightedCapacity()
	})

	engine := req.Engine
	if engine == "" {
		engine = "mock"
	}
	modelName := req.Model
	if modelName == "" {
		if engine == "real" {
			modelName = "Qwen/Qwen2.5-0.5B-Instruct"
		} else {
			modelName = "mock-3b"
		}
	}

	secondary := members[0]
	if len(members) > 1 {
		secondary = members[1]
	}
	job := s.orch.CreateJob(colonyID, modelName, req.Prompt, engine, members[0].ID, secondary.ID, req.MaxNewTokens)
	log.Printf("rest: deployed LLM job %s to colony %s (primary %s, secondary %s)", job.ID, colonyID, members[0].Name, secondary.Name)
	s.logEvent(model.Event{TS: time.Now().UTC(), Level: model.LevelInfo, Category: model.CategoryJob,
		Message: fmt.Sprintf("Deployed %s across %s and %s (%s engine)", modelName, members[0].Name, secondary.Name, engine)})
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, s.orch.Jobs())
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	job, ok := s.orch.Job(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleListErrors(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	errs, err := s.store.ListErrors(limit)
	if err != nil {
		log.Printf("rest: list errors: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list errors")
		return
	}
	writeJSON(w, http.StatusOK, errs)
}

type createFeedbackRequest struct {
	Message string `json:"message"`
	Email   string `json:"email"`
}

// maxFeedbackMessage keeps a single submission from ballooning the database;
// generous enough for a real bug report, not enough for an accidental paste of
// a whole log file.
const maxFeedbackMessage = 4000

func (s *Server) handleCreateFeedback(w http.ResponseWriter, r *http.Request) {
	var req createFeedbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		writeError(w, http.StatusBadRequest, "a message is required")
		return
	}
	if len(req.Message) > maxFeedbackMessage {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("message is too long, keep it under %d characters", maxFeedbackMessage))
		return
	}
	req.Email = strings.TrimSpace(req.Email)

	ts := time.Now().UTC()
	if err := s.store.InsertFeedback(req.Message, req.Email, ts); err != nil {
		log.Printf("rest: insert feedback: %v", err)
		writeError(w, http.StatusInternalServerError, "could not record feedback")
		return
	}
	s.logEvent(model.Event{TS: ts, Level: model.LevelInfo, Category: model.CategorySystem,
		Message: "New feedback submitted from the admin dashboard"})
	writeJSON(w, http.StatusCreated, map[string]string{"status": "received"})
}

func (s *Server) handleListFeedback(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	items, err := s.store.ListFeedback(limit)
	if err != nil {
		log.Printf("rest: list feedback: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list feedback")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("rest: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
