"use client";

import { useMemo } from "react";
import { Banner, Card, StatCard } from "@/components/ui";
import { usePoll } from "@/lib/client";
import type { Colony, Contribution } from "@/lib/types";

const number = (value: number, digits = 2) => value.toLocaleString(undefined, { maximumFractionDigits: digits });

export default function AnalyticsPage() {
  const { data: rows, error } = usePoll<Contribution[]>("/api/contributions", 3000);
  const { data: colonies } = usePoll<Colony[]>("/api/colonies", 3000);
  const totals = useMemo(() => (rows ?? []).reduce((sum, row) => ({
    online: sum.online + row.online_hours,
    active: sum.active + row.active_hours,
    cpu: sum.cpu + row.cpu_core_hours,
    ram: sum.ram + row.ram_gb_hours,
    currentCPU: sum.currentCPU + row.current_cpu_cores,
    currentRAM: sum.currentRAM + row.current_ram_gb,
  }), { online: 0, active: 0, cpu: 0, ram: 0, currentCPU: 0, currentRAM: 0 }), [rows]);
  const zoneName = new Map((colonies ?? []).map((zone) => [zone.id, zone.name]));

  return <div className="space-y-6">
    <div>
      <h1 className="text-xl font-semibold text-colony-navy">Contribution analytics</h1>
      <p className="mt-1 text-sm text-colony-slate">Durable totals from node heartbeats. Live worker draw updates every few seconds.</p>
    </div>
    {error && <Banner text={`Cannot load contribution records: ${error}`} />}
    <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
      <StatCard label="CPU work" value={`${number(totals.cpu)} core-h`} hint={`${number(totals.currentCPU)} cores now`} />
      <StatCard label="Memory work" value={`${number(totals.ram)} GB-h`} hint={`${number(totals.currentRAM)} GB now`} />
      <StatCard label="Inference time" value={`${number(totals.active)} h`} hint="combined member time" />
      <StatCard label="Availability" value={`${number(totals.online)} h`} hint="combined online time" />
    </div>
    <Card title="Member contribution ledger">
      {!rows?.length ? <p className="text-sm text-colony-slate">Contribution totals will appear after the next heartbeat.</p> :
      <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="border-b border-colony-mist text-xs uppercase tracking-wide text-colony-slate"><tr>
          <th className="py-2 pr-4">Member</th><th className="px-3 py-2">Zone</th><th className="px-3 py-2">Now</th>
          <th className="px-3 py-2 text-right">CPU core-h</th><th className="px-3 py-2 text-right">RAM GB-h</th>
          <th className="px-3 py-2 text-right">Inference h</th><th className="pl-3 py-2 text-right">Online h</th>
        </tr></thead>
        <tbody>{rows.map((row) => <tr key={row.node_id} className="border-b border-colony-mist last:border-0">
          <td className="py-3 pr-4 font-medium text-colony-navy">{row.node_name}</td>
          <td className="px-3 py-3 text-colony-slate">{zoneName.get(row.colony_id) ?? "Unassigned"}</td>
          <td className="px-3 py-3"><span className={`rounded px-2 py-1 text-xs ${row.active ? "bg-colony-softblue text-colony-deep" : "bg-colony-lightblue text-colony-slate"}`}>{row.active ? `${number(row.current_cpu_cores)} CPU / ${number(row.current_ram_gb)} GB` : "Idle"}</span></td>
          <td className="px-3 py-3 text-right font-mono text-colony-charcoal">{number(row.cpu_core_hours)}</td>
          <td className="px-3 py-3 text-right font-mono text-colony-charcoal">{number(row.ram_gb_hours)}</td>
          <td className="px-3 py-3 text-right font-mono text-colony-charcoal">{number(row.active_hours)}</td>
          <td className="pl-3 py-3 text-right font-mono text-colony-charcoal">{number(row.online_hours)}</td>
        </tr>)}</tbody>
      </table></div>}
    </Card>
  </div>;
}
