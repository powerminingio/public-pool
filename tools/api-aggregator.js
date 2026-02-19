/**
 * public-pool API aggregator for multi-worker setups.
 *
 * Aggregates:
 *   - GET /api/info  (userAgents + totalHashRate etc are per-process RAM)
 * Optionally you can also aggregate /api/network by adding it below.
 *
 * Proxies everything else to MASTER_API.
 *
 * Env:
 *   AGG_PORT=3337
 *   MASTER_API=http://127.0.0.1:3334
 *   WORKER_APIS=http://127.0.0.1:3335,http://127.0.0.1:3336
 */

const http = require("http");
const { URL } = require("url");

const AGG_PORT = parseInt(process.env.AGG_PORT || "3337", 10);
const MASTER_API = process.env.MASTER_API || "http://127.0.0.1:3334";
const WORKER_APIS = (process.env.WORKER_APIS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function setCors(res, req) {
  const origin = req.headers.origin || "*";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", req.headers["access-control-request-headers"] || "content-type,authorization");
  res.setHeader("Access-Control-Allow-Credentials", "true");
}

function fetchJson(targetUrl) {
  return new Promise((resolve, reject) => {
    const u = new URL(targetUrl);
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        method: "GET",
        timeout: 5000,
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode || 500, json: JSON.parse(body) });
          } catch (e) {
            reject(new Error(`Bad JSON from ${targetUrl}: ${e.message}`));
          }
        });
      }
    );
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error(`Timeout ${targetUrl}`)));
    req.end();
  });
}

function toNum(x) {
  if (x === null || x === undefined) return 0;
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Merge /api/info payloads from multiple workers.
 *
 * Keeps shape compatible with public-pool-ui:
 * - userAgents[].count remains string
 * - userAgents[].totalHashRate remains string
 * - bestDifficulty stays as string if it was string (we emit string)
 * - highScores merged, sorted by updatedAt desc, trimmed
 */
function mergeInfoPayloads(payloads) {
  const out = {
    blockData: [],
    userAgents: [],
    highScores: [],
    uptime: null,
  };

  // blockData: usually empty in your output; just concat if present.
  for (const p of payloads) {
    if (!p) continue;
    if (Array.isArray(p.blockData)) out.blockData.push(...p.blockData);
    if (Array.isArray(p.highScores)) out.highScores.push(...p.highScores);
  }

  // uptime: pick earliest start (or just keep the first non-null)
  // Your field looks like a timestamp; keeping the earliest is usually more sensible.
  const uptimes = payloads.map((p) => p?.uptime).filter(Boolean);
  if (uptimes.length) {
    out.uptime = uptimes.sort((a, b) => new Date(a) - new Date(b))[0];
  }

  // userAgents: merge by name
  const uaMap = new Map();
  for (const p of payloads) {
    for (const ua of p?.userAgents || []) {
      const key = ua.userAgent || "Other";
      const prev = uaMap.get(key) || {
        userAgent: key,
        count: 0,
        bestDifficulty: 0,
        totalHashRate: 0,
      };

      prev.count += toNum(ua.count);
      prev.totalHashRate += toNum(ua.totalHashRate);
      prev.bestDifficulty = Math.max(prev.bestDifficulty, toNum(ua.bestDifficulty));

      uaMap.set(key, prev);
    }
  }

  out.userAgents = [...uaMap.values()]
    // optional: sort by totalHashRate desc for nicer UI
    .sort((a, b) => b.totalHashRate - a.totalHashRate)
    .map((x) => ({
      userAgent: x.userAgent,
      count: String(Math.trunc(x.count)),
      bestDifficulty: String(x.bestDifficulty),
      totalHashRate: String(x.totalHashRate),
    }));

  // highScores: sort by updatedAt desc, keep top 50
  out.highScores.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
  out.highScores = out.highScores.slice(0, 50);

  return out;
}

function proxyToMaster(req, res) {
  const master = new URL(MASTER_API);
  const options = {
    hostname: master.hostname,
    port: master.port,
    path: req.url,
    method: req.method,
    headers: req.headers,
  };

  const upstream = http.request(options, (uRes) => {
    setCors(res, req);
    res.writeHead(uRes.statusCode || 502, uRes.headers);
    uRes.pipe(res);
  });

  upstream.on("error", (e) => {
    setCors(res, req);
    res.writeHead(502, { "content-type": "text/plain" });
    res.end(`Upstream error: ${e.message}`);
  });

  req.pipe(upstream);
}

function proxyOnce(upstreamBase, req, res) {
  return new Promise((resolve) => {
    const upstream = new URL(upstreamBase);
    const options = {
      hostname: upstream.hostname,
      port: upstream.port,
      path: req.url,
      method: req.method,
      headers: req.headers,
    };

    const uReq = http.request(options, (uRes) => {
      const ct = (uRes.headers["content-type"] || "").toLowerCase();
      resolve({ status: uRes.statusCode || 502, contentType: ct, uRes });
    });

    uReq.on("error", () => resolve({ status: 0, contentType: "", uRes: null }));
    req.pipe(uReq);
  });
}

async function smartProxy(req, res) {
  // Try master
  const m = await proxyOnce(MASTER_API, req, res);
  if (m.uRes && m.status >= 200 && m.status < 300 && !m.contentType.includes("text/html")) {
    setCors(res, req);
    res.writeHead(m.status, m.uRes.headers);
    m.uRes.pipe(res);
    return true;
  }

  // If master gave HTML or failed, try first healthy worker
  for (const w of WORKER_APIS) {
    const wRes = await proxyOnce(w, req, res);
    if (wRes.uRes && wRes.status >= 200 && wRes.status < 300 && !wRes.contentType.includes("text/html")) {
      setCors(res, req);
      res.writeHead(wRes.status, wRes.uRes.headers);
      wRes.uRes.pipe(res);
      return true;
    }
  }

  // Last resort: return whatever master gave us (even HTML) or 502
  if (m.uRes) {
    setCors(res, req);
    res.writeHead(m.status || 502, m.uRes.headers);
    m.uRes.pipe(res);
    return true;
  }
  setCors(res, req);
  res.writeHead(502, { "content-type": "text/plain" });
  res.end("Upstream error: no healthy upstream returned JSON");
  return true;
}

const server = http.createServer(async (req, res) => {

  const u = new URL(req.url, "http://localhost");
  const path = u.pathname;

  if (req.method === "GET" && path === "/api/info") {
    const results = await Promise.all(
      WORKER_APIS.map((base) =>
        fetchJson(`${base}/api/info`)
          .then((r) => r.json)
          .catch(() => null)
      )
    );

    const ok = results.filter(Boolean);
    if (ok.length === 0) return smartProxy(req, res);

    const merged = mergeInfoPayloads(ok);
    setCors(res, req);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(merged));
    return;
  }
  else if (req.method === "OPTIONS") {
    setCors(res, req);
    res.writeHead(204);
    res.end();
    return;
  }

  return smartProxy(req, res);
});

server.listen(AGG_PORT, "0.0.0.0", () => {
  console.log(`API aggregator listening on :${AGG_PORT}`);
  console.log(`MASTER_API=${MASTER_API}`);
  console.log(`WORKER_APIS=${WORKER_APIS.join(",")}`);
});
