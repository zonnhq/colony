"use client";

import Link from "next/link";
import { usePoll } from "@/lib/client";
import { Card, Banner, LevelBadge } from "@/components/ui";
import type { Job, JobEvent, Node, Contribution } from "@/lib/types";

export default function JobPage({ params }: { params: { id: string } }) {
  const { data: job, error } = usePoll<Job>(`/api/jobs/${params.id}`, 1500);
  const { data: nodes } = usePoll<Node[]>("/api/nodes", 2000);
  const { data: contributions } = usePoll<Contribution[]>("/api/contributions", 3000);

  const nodeById = (id: string) => (nodes ?? []).find((n) => n.id === id);
  const contributionById = (id: string) => (contributions ?? []).find((c) => c.node_id === id);
  const primary = job ? nodeById(job.primary_node_id) : undefined;
  const secondary = job ? nodeById(job.secondary_node_id) : undefined;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Link href="/colonies" className="text-sm text-colony-core hover:underline">Zones</Link>
        <span className="text-colony-mist">/</span>
        <h1 className="text-xl font-semibold text-colony-navy">LLM job</h1>
        {job && <JobStatus status={job.status} />}
      </div>

      {error && <Banner text={`Cannot load this job: ${error}`} />}

      {job && (
        <>
          <Card title="Prompt">
            <p className="text-sm text-colony-charcoal">{job.prompt}</p>
            <p className="mt-2 text-xs text-colony-slate">Engine {job.engine} · Model {job.model}</p>
          </Card>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <WorkerCard title="Primary node (lower layers)" node={primary} />
            <WorkerCard title="Secondary node (upper layers)" node={secondary} />
          </div>

          <Card title="Result">
            {job.status === "DONE" ? (
              <p className="whitespace-pre-wrap text-sm text-colony-charcoal">
                <span className="text-colony-slate">{job.prompt}</span> {job.result}
              </p>
            ) : job.status === "FAILED" ? (
              <p className="text-sm text-colony-indigo">{job.error || "The job failed."}</p>
            ) : (
              <p className="text-sm text-colony-slate">
                Waiting for the split inference to finish. The primary node is relaying activation tensors through the Coordinator to the secondary node.
              </p>
            )}
          </Card>

          <Card title="Node contributions">
            <p className="mb-3 text-xs text-colony-slate">
              This is not one machine doing the work: the primary computes the lower layers and the secondary
              the upper layers, and each row below is what that specific node actually carried, live.
            </p>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-colony-mist text-xs uppercase tracking-wide text-colony-slate">
                    <th className="py-2 pr-4">Node</th>
                    <th className="py-2 pr-4">Role</th>
                    <th className="py-2 pr-4">Tensors relayed</th>
                    <th className="py-2 pr-4">Live draw</th>
                    <th className="py-2 pr-4">Lifetime CPU-hours</th>
                    <th className="py-2 pr-4">Lifetime RAM-hours</th>
                  </tr>
                </thead>
                <tbody>
                  <ContributionRow
                    node={primary}
                    role="Primary"
                    tensors={job.primary_tensors_relayed}
                    contribution={contributionById(job.primary_node_id)}
                  />
                  <ContributionRow
                    node={secondary}
                    role="Secondary"
                    tensors={job.secondary_tensors_relayed}
                    contribution={contributionById(job.secondary_node_id)}
                  />
                </tbody>
              </table>
            </div>
          </Card>

          <Card title="Node activity log">
            <p className="mb-3 text-xs text-colony-slate">
              Every event either node has reported for this job, in order, so what each machine is doing is
              never a black box.
            </p>
            <EventLog events={job.events} nodeById={nodeById} />
          </Card>
        </>
      )}
    </div>
  );
}

function JobStatus({ status }: { status: string }) {
  const map: Record<string, string> = {
    PENDING: "bg-colony-mist text-colony-navy",
    RUNNING: "bg-colony-softblue text-colony-deep",
    DONE: "bg-colony-ice text-colony-midnight",
    FAILED: "bg-colony-indigo text-colony-cloud",
  };
  const cls = map[status] ?? "bg-colony-mist text-colony-navy";
  return <span className={`rounded-full px-3 py-0.5 text-xs font-medium ${cls}`}>{status}</span>;
}

function WorkerCard({ title, node }: { title: string; node?: Node }) {
  return (
    <Card title={title}>
      {node ? (
        <div className="space-y-1 text-sm">
          <div className="flex justify-between"><span className="text-colony-slate">Node</span><span>{node.name}</span></div>
          <div className="flex justify-between"><span className="text-colony-slate">Status</span><span>{node.status}</span></div>
          <div className="flex justify-between"><span className="text-colony-slate">CPU in use</span><span className="font-mono text-xs">{node.utilization.cpu_used.toFixed(1)} cores</span></div>
          <div className="flex justify-between"><span className="text-colony-slate">GPU memory</span><span className="font-mono text-xs">{node.utilization.gpu_mem_used_gb.toFixed(1)} GB</span></div>
        </div>
      ) : (
        <p className="text-sm text-colony-slate">Node not found.</p>
      )}
    </Card>
  );
}

function ContributionRow({
  node,
  role,
  tensors,
  contribution,
}: {
  node?: Node;
  role: string;
  tensors?: number;
  contribution?: Contribution;
}) {
  return (
    <tr className="border-b border-colony-mist last:border-0">
      <td className="py-2 pr-4">{node?.name ?? "Not connected yet"}</td>
      <td className="py-2 pr-4 text-colony-slate">{role}</td>
      <td className="py-2 pr-4 font-mono text-xs">{tensors ?? 0}</td>
      <td className="py-2 pr-4 font-mono text-xs">
        {contribution?.active
          ? `${contribution.current_cpu_cores.toFixed(2)} cores · ${contribution.current_ram_gb.toFixed(2)} GB`
          : "idle"}
      </td>
      <td className="py-2 pr-4 font-mono text-xs">{(contribution?.cpu_core_hours ?? 0).toFixed(3)}</td>
      <td className="py-2 pr-4 font-mono text-xs">{(contribution?.ram_gb_hours ?? 0).toFixed(3)}</td>
    </tr>
  );
}

function EventLog({
  events,
  nodeById,
}: {
  events?: JobEvent[];
  nodeById: (id: string) => Node | undefined;
}) {
  if (!events || events.length === 0) {
    return <p className="text-sm text-colony-slate">No activity recorded yet.</p>;
  }
  return (
    <div className="max-h-96 space-y-1 overflow-y-auto text-sm">
      {events
        .slice()
        .reverse()
        .map((e, i) => (
          <div key={i} className="flex items-start gap-3 border-b border-colony-mist py-2 last:border-0">
            <span className="w-20 shrink-0 font-mono text-xs text-colony-slate">
              {new Date(e.ts).toLocaleTimeString()}
            </span>
            <span className="w-14 shrink-0">
              <LevelBadge level={e.level} />
            </span>
            <span className="w-36 shrink-0 truncate text-xs text-colony-slate">
              {nodeById(e.node_id)?.name ?? e.node_id.slice(0, 10)}
              <span className="ml-1 text-colony-mist">({e.role})</span>
            </span>
            <span className="text-colony-charcoal">{e.message}</span>
          </div>
        ))}
    </div>
  );
}
