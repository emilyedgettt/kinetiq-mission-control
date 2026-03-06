import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

type RecordResponse =
  | {
      ok: true;
    }
  | {
      ok: false;
      code: "INVALID" | "COOLDOWN";
      message: string;
      remaining_attempts?: number;
      retry_at?: string;
    };

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const MAX_ATTEMPTS = 5;
const COOLDOWN_MINUTES = 5;
const IP_CAP_MAX = 30;
const CALLSIGN_PATTERN = /^[a-z0-9_-]{3,24}$/;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const json = (body: RecordResponse, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "content-type": "application/json" },
  });

const getClient = () =>
  createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

const getIp = (req: Request) => {
  const headers = req.headers;
  const cf = headers.get("cf-connecting-ip");
  if (cf) return cf;
  const xff = headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0]?.trim();
  const xr = headers.get("x-real-ip");
  if (xr) return xr;
  const xci = headers.get("x-client-ip");
  if (xci) return xci;
  return "unknown";
};

const sha256Hex = async (input: string) => {
  const data = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
};

const formatCooldownMessage = (retryAt: Date) => {
  const iso = retryAt.toISOString();
  return `Launch cooldown active. Retry at ${iso}.`;
};

const invalidMessage = (remaining: number) =>
  `Invalid Credentials. Launch will be aborted in ${remaining} attempts.`;

const upsertThrottle = async (
  supabase: ReturnType<typeof createClient>,
  payload: {
    throttle_key: string;
    callsign: string;
    ip_hash: string;
    failed_count: number;
    cooldown_until: string | null;
    window_start: string;
    updated_at: string;
  },
) => {
  const { error } = await supabase.schema("mc_auth").from("mc_login_throttle").upsert(payload, {
    onConflict: "throttle_key",
  });
  if (error) throw error;
};

const fetchThrottle = async (
  supabase: ReturnType<typeof createClient>,
  key: string,
) => {
  const { data, error } = await supabase
    .schema("mc_auth")
    .from("mc_login_throttle")
    .select("*")
    .eq("throttle_key", key)
    .maybeSingle();
  if (error) throw error;
  return data;
};

const handleFailure = async (
  supabase: ReturnType<typeof createClient>,
  params: {
    key: string;
    callsign: string;
    ip_hash: string;
  },
) => {
  const now = new Date();
  const nowIso = now.toISOString();
  const row = await fetchThrottle(supabase, params.key);
  let failedCount = 1;
  let windowStart = now;
  let cooldownUntil: Date | null = null;

  if (row) {
    const ws = new Date(row.window_start);
    const windowExpired = now.getTime() - ws.getTime() > COOLDOWN_MINUTES * 60_000;
    if (windowExpired) {
      failedCount = 1;
      windowStart = now;
      cooldownUntil = null;
    } else {
      failedCount = (row.failed_count ?? 0) + 1;
      windowStart = ws;
      cooldownUntil = row.cooldown_until ? new Date(row.cooldown_until) : null;
    }
  }

  if (failedCount >= MAX_ATTEMPTS) {
    cooldownUntil = new Date(now.getTime() + COOLDOWN_MINUTES * 60_000);
  }

  await upsertThrottle(supabase, {
    throttle_key: params.key,
    callsign: params.callsign,
    ip_hash: params.ip_hash,
    failed_count: failedCount,
    cooldown_until: cooldownUntil ? cooldownUntil.toISOString() : null,
    window_start: windowStart.toISOString(),
    updated_at: nowIso,
  });

  return { failedCount, cooldownUntil };
};

const resetThrottle = async (
  supabase: ReturnType<typeof createClient>,
  params: { key: string; callsign: string; ip_hash: string },
) => {
  const nowIso = new Date().toISOString();
  await upsertThrottle(supabase, {
    throttle_key: params.key,
    callsign: params.callsign,
    ip_hash: params.ip_hash,
    failed_count: 0,
    cooldown_until: null,
    window_start: nowIso,
    updated_at: nowIso,
  });
};

const checkCooldown = (row: { cooldown_until?: string | null }) => {
  if (!row?.cooldown_until) return null;
  const cooldown = new Date(row.cooldown_until);
  if (cooldown.getTime() > Date.now()) return cooldown;
  return null;
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return json({ ok: false, code: "INVALID", message: "Method not allowed" }, 405);
  }

  const { callsign, success } = await req.json().catch(() => ({
    callsign: "",
    success: false,
  }));

  const normalized = String(callsign ?? "").trim().toLowerCase();
  const isSuccess = Boolean(success);

  const ip = getIp(req);
  const ipHash = await sha256Hex(ip);
  const key = await sha256Hex(`${normalized}|${ipHash}`);
  const keyIp = await sha256Hex(`*|${ipHash}`);

  const supabase = getClient();

  const row = await fetchThrottle(supabase, key);
  const rowIp = await fetchThrottle(supabase, keyIp);

  const cooldown = checkCooldown(row) ?? checkCooldown(rowIp);
  if (cooldown && !isSuccess) {
    return json({
      ok: false,
      code: "COOLDOWN",
      message: formatCooldownMessage(cooldown),
      retry_at: cooldown.toISOString(),
    });
  }

  if (!CALLSIGN_PATTERN.test(normalized)) {
    const { failedCount, cooldownUntil } = await handleFailure(supabase, {
      key,
      callsign: normalized,
      ip_hash: ipHash,
    });
    await handleFailure(supabase, { key: keyIp, callsign: "*", ip_hash: ipHash });
    if (cooldownUntil) {
      return json({
        ok: false,
        code: "COOLDOWN",
        message: formatCooldownMessage(cooldownUntil),
        retry_at: cooldownUntil.toISOString(),
      });
    }
    const remaining = Math.max(0, MAX_ATTEMPTS - failedCount);
    return json({
      ok: false,
      code: "INVALID",
      remaining_attempts: remaining,
      message: invalidMessage(remaining),
    });
  }

  if (isSuccess) {
    await resetThrottle(supabase, {
      key,
      callsign: normalized,
      ip_hash: ipHash,
    });
    return json({ ok: true });
  }

  const { failedCount, cooldownUntil } = await handleFailure(supabase, {
    key,
    callsign: normalized,
    ip_hash: ipHash,
  });
  await handleFailure(supabase, { key: keyIp, callsign: "*", ip_hash: ipHash });

  if (cooldownUntil) {
    return json({
      ok: false,
      code: "COOLDOWN",
      message: formatCooldownMessage(cooldownUntil),
      retry_at: cooldownUntil.toISOString(),
    });
  }

  const remaining = Math.max(0, MAX_ATTEMPTS - failedCount);
  return json({
    ok: false,
    code: "INVALID",
    remaining_attempts: remaining,
    message: invalidMessage(remaining),
  });
});
