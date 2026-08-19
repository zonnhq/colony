package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/projectcolony/colony/coordinator/internal/model"
)

// UpsertNode inserts a node or updates its mutable fields on re-registration.
// created_at is preserved on conflict.
func (d *DB) UpsertNode(n model.Node) error {
	_, err := d.sql.Exec(`
		INSERT INTO nodes (
			id, name, os, arch,
			cpu_cores, ram_gb, gpu_model, gpu_memory_gb, disk_gb,
			alloc_cpu_cores, alloc_ram_gb, alloc_gpu_memory_gb, alloc_bandwidth_mbps,
			status, colony_id, last_seen, fingerprint
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			os = excluded.os,
			arch = excluded.arch,
			cpu_cores = excluded.cpu_cores,
			ram_gb = excluded.ram_gb,
			gpu_model = excluded.gpu_model,
			gpu_memory_gb = excluded.gpu_memory_gb,
			disk_gb = excluded.disk_gb,
			alloc_cpu_cores = excluded.alloc_cpu_cores,
			alloc_ram_gb = excluded.alloc_ram_gb,
			alloc_gpu_memory_gb = excluded.alloc_gpu_memory_gb,
			alloc_bandwidth_mbps = excluded.alloc_bandwidth_mbps,
			status = excluded.status,
			last_seen = excluded.last_seen,
			fingerprint = excluded.fingerprint`,
		n.ID, n.Name, n.OS, n.Arch,
		n.Resources.CPUCores, n.Resources.RAMGB, n.Resources.GPUModel, n.Resources.GPUMemory, n.Resources.DiskGB,
		n.Allocated.CPUCores, n.Allocated.RAMGB, n.Allocated.GPUMemory, n.Allocated.BandwidthMbps,
		n.Status, nullString(n.ColonyID), n.LastSeen, n.Fingerprint,
	)
	if err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}
	return nil
}

// FindNodeIDByFingerprint returns the id of the node with the given fingerprint,
// or empty string if none. A blank fingerprint never matches.
func (d *DB) FindNodeIDByFingerprint(fingerprint string) (string, error) {
	if fingerprint == "" {
		return "", nil
	}
	var id string
	err := d.sql.QueryRow(
		`SELECT id FROM nodes WHERE fingerprint = ? ORDER BY created_at ASC LIMIT 1`, fingerprint,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

// RecordHeartbeat updates the node's live state and appends a heartbeat row.
func (d *DB) RecordHeartbeat(nodeID string, u model.Utilization, colonyID string, ts time.Time, workerCPU, workerRAM float64, workerActive bool) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`UPDATE nodes SET status = ?, last_seen = ? WHERE id = ?`,
		model.StatusOnline, ts, nodeID,
	); err != nil {
		return fmt.Errorf("update node on heartbeat: %w", err)
	}

	var previous sql.NullTime
	_ = tx.QueryRow(`SELECT updated_at FROM contribution_totals WHERE node_id = ?`, nodeID).Scan(&previous)
	delta := 0.0
	if previous.Valid {
		delta = ts.Sub(previous.Time).Seconds()
		if delta < 0 || delta > 30 {
			delta = 0
		}
	}
	active := 0
	if workerActive {
		active = 1
	}
	if _, err := tx.Exec(`
		INSERT INTO contribution_totals (
			node_id, colony_id, online_seconds, active_seconds, cpu_core_seconds,
			ram_gb_seconds, current_cpu_cores, current_ram_gb, worker_active, samples, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT(node_id) DO UPDATE SET
			colony_id = excluded.colony_id,
			online_seconds = contribution_totals.online_seconds + excluded.online_seconds,
			active_seconds = contribution_totals.active_seconds + excluded.active_seconds,
			cpu_core_seconds = contribution_totals.cpu_core_seconds + excluded.cpu_core_seconds,
			ram_gb_seconds = contribution_totals.ram_gb_seconds + excluded.ram_gb_seconds,
			current_cpu_cores = excluded.current_cpu_cores,
			current_ram_gb = excluded.current_ram_gb,
			worker_active = excluded.worker_active,
			samples = contribution_totals.samples + 1,
			updated_at = excluded.updated_at`,
		nodeID, nullString(colonyID), delta, delta*float64(active), workerCPU*delta,
		workerRAM*delta, workerCPU, workerRAM, active, ts,
	); err != nil {
		return fmt.Errorf("update contribution totals: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT INTO heartbeats (node_id, ts, cpu_used, ram_used_gb, gpu_mem_used_gb, gpu_temp_c, colony_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		nodeID, ts, u.CPUUsed, u.RAMUsedGB, u.GPUMemUsedGB, u.GPUTempC, nullString(colonyID),
	); err != nil {
		return fmt.Errorf("insert heartbeat: %w", err)
	}

	return tx.Commit()
}

// ListContributions returns durable totals for every node, optionally limited to a Zone.
func (d *DB) ListContributions(colonyID string) ([]model.Contribution, error) {
	query := `SELECT c.node_id, n.name, COALESCE(c.colony_id, ''), c.online_seconds / 3600.0,
		c.active_seconds / 3600.0, c.cpu_core_seconds / 3600.0, c.ram_gb_seconds / 3600.0,
		c.current_cpu_cores, c.current_ram_gb, c.worker_active, c.samples, c.updated_at
		FROM contribution_totals c JOIN nodes n ON n.id = c.node_id`
	var args []any
	if colonyID != "" {
		query += ` WHERE c.colony_id = ?`
		args = append(args, colonyID)
	}
	query += ` ORDER BY c.cpu_core_seconds + c.ram_gb_seconds DESC`
	rows, err := d.sql.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Contribution
	for rows.Next() {
		var c model.Contribution
		var active int
		if err := rows.Scan(&c.NodeID, &c.NodeName, &c.ColonyID, &c.OnlineHours, &c.ActiveHours,
			&c.CPUCoreHours, &c.RAMGBHours, &c.CurrentCPUCores, &c.CurrentRAMGB,
			&active, &c.Samples, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Active = active != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetNodeStatus updates only the status column.
func (d *DB) SetNodeStatus(nodeID, status string) error {
	_, err := d.sql.Exec(`UPDATE nodes SET status = ? WHERE id = ?`, status, nodeID)
	return err
}

// NodeExists reports whether a node id is known.
func (d *DB) NodeExists(nodeID string) (bool, error) {
	var one int
	err := d.sql.QueryRow(`SELECT 1 FROM nodes WHERE id = ?`, nodeID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListNodes returns every node with its last known state.
func (d *DB) ListNodes() ([]model.Node, error) {
	rows, err := d.sql.Query(`
		SELECT id, name, os, arch,
		       cpu_cores, ram_gb, gpu_model, gpu_memory_gb, disk_gb,
		       alloc_cpu_cores, alloc_ram_gb, alloc_gpu_memory_gb, alloc_bandwidth_mbps,
		       status, colony_id, last_seen, created_at, fingerprint
		FROM nodes
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func scanNode(rows *sql.Rows) (model.Node, error) {
	var n model.Node
	var colonyID sql.NullString
	var lastSeen sql.NullTime
	err := rows.Scan(
		&n.ID, &n.Name, &n.OS, &n.Arch,
		&n.Resources.CPUCores, &n.Resources.RAMGB, &n.Resources.GPUModel, &n.Resources.GPUMemory, &n.Resources.DiskGB,
		&n.Allocated.CPUCores, &n.Allocated.RAMGB, &n.Allocated.GPUMemory, &n.Allocated.BandwidthMbps,
		&n.Status, &colonyID, &lastSeen, &n.CreatedAt, &n.Fingerprint,
	)
	if err != nil {
		return n, err
	}
	if colonyID.Valid {
		n.ColonyID = colonyID.String
	}
	if lastSeen.Valid {
		t := lastSeen.Time
		n.LastSeen = &t
	}
	return n, nil
}

// CreateColony inserts a colony and its membership in one transaction, and
// points the member nodes at the new colony.
func (d *DB) CreateColony(c model.Colony) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO colonies (id, name, status, created_at) VALUES (?, ?, ?, ?)`,
		c.ID, c.Name, model.ColonyActive, c.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert colony: %w", err)
	}

	for _, nodeID := range c.NodeIDs {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO colony_nodes (colony_id, node_id) VALUES (?, ?)`,
			c.ID, nodeID,
		); err != nil {
			return fmt.Errorf("insert membership: %w", err)
		}
		if _, err := tx.Exec(
			`UPDATE nodes SET colony_id = ? WHERE id = ?`, c.ID, nodeID,
		); err != nil {
			return fmt.Errorf("assign node colony: %w", err)
		}
	}

	return tx.Commit()
}

// DeleteColony marks a colony deleted and releases its nodes back to idle.
func (d *DB) DeleteColony(colonyID string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`UPDATE colonies SET status = ? WHERE id = ?`, model.ColonyDeleted, colonyID,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE nodes SET colony_id = NULL WHERE colony_id = ?`, colonyID,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM colony_nodes WHERE colony_id = ?`, colonyID,
	); err != nil {
		return err
	}

	return tx.Commit()
}

// ListColonies returns active colonies with their member node ids.
func (d *DB) ListColonies() ([]model.Colony, error) {
	rows, err := d.sql.Query(
		`SELECT id, name, status, created_at FROM colonies WHERE status = ? ORDER BY created_at DESC`,
		model.ColonyActive,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Colony{}
	for rows.Next() {
		var c model.Colony
		if err := rows.Scan(&c.ID, &c.Name, &c.Status, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range out {
		ids, err := d.colonyMembers(out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].NodeIDs = ids
	}
	return out, nil
}

// ColonyExists reports whether an active colony id is known.
func (d *DB) ColonyExists(colonyID string) (bool, error) {
	var one int
	err := d.sql.QueryRow(
		`SELECT 1 FROM colonies WHERE id = ? AND status = ?`, colonyID, model.ColonyActive,
	).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *DB) colonyMembers(colonyID string) ([]string, error) {
	rows, err := d.sql.Query(`SELECT node_id FROM colony_nodes WHERE colony_id = ?`, colonyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// InsertError appends an entry to the issues feed.
func (d *DB) InsertError(nodeID, level, message string, ts time.Time) error {
	_, err := d.sql.Exec(
		`INSERT INTO errors (node_id, level, message, ts) VALUES (?, ?, ?, ?)`,
		nodeID, level, message, ts,
	)
	return err
}

// ListErrors returns the most recent issues feed entries, newest first.
func (d *DB) ListErrors(limit int) ([]model.NodeError, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.sql.Query(
		`SELECT id, node_id, level, message, ts FROM errors ORDER BY ts DESC, id DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.NodeError{}
	for rows.Next() {
		var e model.NodeError
		if err := rows.Scan(&e.ID, &e.NodeID, &e.Level, &e.Message, &e.TS); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// InsertEvent appends an entry to the full activity log.
func (d *DB) InsertEvent(e model.Event) error {
	_, err := d.sql.Exec(
		`INSERT INTO events (ts, level, category, node_id, node_name, message) VALUES (?, ?, ?, ?, ?, ?)`,
		e.TS, e.Level, e.Category, e.NodeID, e.NodeName, e.Message,
	)
	return err
}

// ListEvents returns the most recent activity entries, newest first.
func (d *DB) ListEvents(limit int) ([]model.Event, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := d.sql.Query(
		`SELECT id, ts, level, category, node_id, node_name, message
		 FROM events ORDER BY ts DESC, id DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Event{}
	for rows.Next() {
		var e model.Event
		if err := rows.Scan(&e.ID, &e.TS, &e.Level, &e.Category, &e.NodeID, &e.NodeName, &e.Message); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// InsertFeedback appends a submission from the admin dashboard's feedback form.
func (d *DB) InsertFeedback(message, email string, ts time.Time) error {
	_, err := d.sql.Exec(
		`INSERT INTO feedback (ts, message, email) VALUES (?, ?, ?)`,
		ts, message, email,
	)
	return err
}

// ListFeedback returns the most recent feedback submissions, newest first.
func (d *DB) ListFeedback(limit int) ([]model.Feedback, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.sql.Query(
		`SELECT id, ts, message, email FROM feedback ORDER BY ts DESC, id DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Feedback{}
	for rows.Next() {
		var f model.Feedback
		if err := rows.Scan(&f.ID, &f.TS, &f.Message, &f.Email); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
