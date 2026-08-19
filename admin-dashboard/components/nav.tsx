"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Fleet" },
  { href: "/nodes", label: "Nodes" },
  { href: "/colonies", label: "Zones" },
  { href: "/analytics", label: "Analytics" },
  { href: "/playground", label: "Playground" },
  { href: "/activity", label: "Activity" },
  { href: "/issues", label: "Issues" },
  { href: "/feedback", label: "Feedback" },
];

export function Nav() {
  const pathname = usePathname();

  const logout = async () => {
    await fetch("/api/logout", { method: "POST" });
    window.location.href = "/login";
  };

  const isActive = (href: string) => (href === "/" ? pathname === "/" : pathname.startsWith(href));

  return (
    <header className="bg-colony-navy">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-3">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <div className="h-7 w-7 rounded bg-colony-core" />
            <span className="font-semibold text-colony-cloud">Zonn Console</span>
          </div>
          <nav className="flex gap-1">
            {links.map((l) => (
              <Link
                key={l.href}
                href={l.href}
                className={`rounded px-3 py-1.5 text-sm font-medium ${
                  isActive(l.href) ? "bg-colony-deep text-colony-cloud" : "text-colony-mist hover:text-colony-cloud"
                }`}
              >
                {l.label}
              </Link>
            ))}
          </nav>
        </div>
        <button onClick={logout} className="text-sm text-colony-mist hover:text-colony-cloud">
          Sign out
        </button>
      </div>
    </header>
  );
}
