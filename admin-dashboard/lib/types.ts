// Types mirror the Coordinator REST JSON, which follows blueprint_v2 section 2.6.

export interface Resources {
  cpu_cores: number;
  ram_gb: number;
  gpu_model: string;
  gpu_memory_gb: number;
  disk_gb: number;
}

export interface Allocation {
  cpu_cores: number;
  ram_gb: number;
  gpu_memory_gb: number;
  bandwidth_mbps: number;
}

export interface Utilization {
  cpu_used: number;
  ram_used_gb: number;
  gpu_mem_used_gb: number;
  gpu_temp_c: number;
}

export interface Node {
  id: string;
  name: string;
  os: string;
  arch: string;
  resources: Resources;
  allocated: Allocation;
  status: "ONLINE" | "BUSY" | "OFFLINE";
  colony_id: string;
  last_seen: string | null;
  created_at: string;
  utilization: Utilization;
  contribution_score: number;
  compute_units: number;
}

export interface Colony {
  id: string;
  name: string;
  node_ids: string[];
  status: string;
  created_at: string;
}

export interface Stats {
  total_nodes: number;
  online_nodes: number;
  offline_nodes: number;
  busy_nodes: number;
  total_cpu_cores: number;
  total_ram_gb: number;
  total_gpus: number;
  total_compute_units: number;
  total_gpu_memory_gb: number;
  gpu_nodes: number;
  cpu_only_nodes: number;
  active_colonies: number;
}

export interface NodeError {
  id: number;
  node_id: string;
  level: "INFO" | "WARN" | "ERROR";
  message: string;
  ts: string;
}

export interface Event {
  id: number;
  ts: string;
  level: "INFO" | "WARN" | "ERROR";
  category: "node" | "colony" | "job" | "system";
  node_id: string;
  node_name: string;
  message: string;
}

export interface Feedback {
  id: number;
  ts: string;
  message: string;
  email: string;
}

export interface JobEvent {
  ts: string;
  node_id: string;
  role: "primary" | "secondary";
  level: "INFO" | "WARN" | "ERROR";
  message: string;
}

export interface Job {
  id: string;
  colony_id: string;
  model: string;
  prompt: string;
  engine: string;
  primary_node_id: string;
  secondary_node_id: string;
  status: "PENDING" | "RUNNING" | "DONE" | "FAILED";
  progress?: string;
  result: string;
  error: string;
  created_at: string;
  updated_at: string;
  primary_tensors_relayed?: number;
  secondary_tensors_relayed?: number;
  events?: JobEvent[];
}

export interface Contribution {
  node_id: string;
  node_name: string;
  colony_id: string;
  online_hours: number;
  active_hours: number;
  cpu_core_hours: number;
  ram_gb_hours: number;
  current_cpu_cores: number;
  current_ram_gb: number;
  active: boolean;
  samples: number;
  updated_at: string;
}
