import { NextRequest, NextResponse } from "next/server";
import { isAuthenticated } from "@/lib/auth";
import { getJSON } from "@/lib/coordinator";

export async function GET(req: NextRequest) {
  if (!isAuthenticated()) return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  try {
    const colony = req.nextUrl.searchParams.get("colony_id");
    const query = colony ? `?colony_id=${encodeURIComponent(colony)}` : "";
    return NextResponse.json(await getJSON(`/api/v1/contributions${query}`));
  } catch (e) {
    return NextResponse.json({ error: String(e) }, { status: 502 });
  }
}
