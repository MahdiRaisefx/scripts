#!/usr/bin/env node

require("dotenv").config();
const express = require("express");
const axios = require("axios");
const fs = require("fs").promises;
const path = require("path");
const crypto = require("crypto");
const Joi = require("joi");
const Bottleneck = require("bottleneck");

// === Configuration ===
const CELLXPERT_BASE_URL = "https://adminapi.cellxpert.com/";
const CUSTOMER_API_URL = process.env.BACKEND_BASE_URL + "/email-by-id";
const ADMIN_URL = "RaiseFX";
const CONTENT_TYPE = "application/x-www-form-urlencoded";
const PORT = process.env.PORT || 3000;
const PULL_INTERVAL_MIN = parseInt(process.env.PULL_INTERVAL_MINUTES, 10) || 15;
const AFFILIATE_ID = process.env.AFFILIATE_ID;
const REPORTS_API_KEY = process.env.REPORTS_API_KEY;

// Vos tokens pour l’API customer
const TOKENS = [
  "148286:NjXtMv0VDGfwDQMAYILHW8z9s1j8UEya",
  "148286:m0a4kDXnhm9BF09Q7CATiKjO5x6mKoM1",
  "148286:aCUsxDM8ZJtr4EW9c77rHOgWNtmGgpEl",
  "148286:pc6kV2GAJml4XIy8d3tmwTUa4iDU7D21",
  "148286:OgkJMF5jXfdpfZ60EcEyZ1uCSq0WuNHV",
  "148286:DxjHa3BnifzyQCir2iUxygfVQCgzGLnN",
  "148286:QsIWPX44BohN8b7Gl6zpsOSbxfxKHZjI",
  "148286:eg7ZTAzvSk43xPiq5nCVourhOQE59L52",
  "148286:3q4qdcBGzBZUSYZmOKQlREp4vv9zsRFS",
  "148286:XIDZae5ChkmarVla9NpyIcXIrjd29Omk",
  "148286:0EUE4UNA0X9iVTcD0O08vE5dE99ZEshx",
  "148286:kMoxnPbu69ak4PsUj07K4bWHuolItiOD",
  "148286:rh3tsLxQiq9QuB2YGBnx9pcI5abhK5um",
  "148286:TybLFvvNdhJHxeFBmDcR4mhOc7GUYRiK",
  "148286:IGa9GLv7VtDN1phDJis8pBcnUjkc6VFj",
  "148286:2bNTrkyIkQpGUOk8PAe9RQOgnhEp6bRv",
  "148286:Z2tq0MdE76COWlnbkDwQYZN2ggxY1ZYd",
  "148286:DIyI8IALc3gAzIuMcmUAWOeWLbbAnm8e",
  "148286:0m6ZLGRSRvh2p16rg7rTxMkGgQ6xkXy1",
  "148286:eM2GjXUNh0DFDaoleBKQ6h7do7o13j1g",
];

// Crée un limiter Bottleneck par token : max 60 requêtes par minute, 1 à la fois
const tokenLimiters = TOKENS.map((token) => ({
  token,
  limiter: new Bottleneck({
    reservoir: 60,
    reservoirRefreshAmount: 60,
    reservoirRefreshInterval: 60 * 1000,
    maxConcurrent: 1,
  }),
}));

let rrIndex = 0; // pour le round-robin

// === Fichiers et schéma ===
const DATA_DIR = path.resolve(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "data.json");
const STATE_FILE = path.join(DATA_DIR, "state.json");
const ACCESS_FILE = path.join(DATA_DIR, "last-client-access.json");
const ERROR_LOG = path.join(DATA_DIR, "error.log");

const username = process.env.API_USERNAME;
const password = process.env.API_PASSWORD;
const serverStart = Date.now();
let isRunning = false;

const recordSchema = Joi.object({
  User_ID: Joi.string().required(),
  Customer_Name: Joi.string().required(),
  Registration_Date: Joi.string().isoDate().required(),
  LOTS: Joi.number().empty(null).default(0).required(),
  First_Deposit: Joi.number().empty(null).default(0),
  First_Deposit_Date: Joi.string().isoDate().allow(null),
  Qualification_Date: Joi.string().isoDate().allow(null),
  Withdrawals: Joi.number().empty(null).default(0).required(),
  PL: Joi.number().empty(null).default(0).required(),
  Commissions: Joi.number().empty(null).default(0),
  TrackingCode: Joi.string().allow(null),
  Tracking_Code: Joi.string().allow(null),
}).unknown(true);

async function loadJSON(filePath, defaultValue) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return defaultValue;
  }
}

async function saveJSON(filePath, data) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2));
}

async function loadClientAccess() {
  const state = await loadJSON(ACCESS_FILE, { lastClientFetch: null });
  return state.lastClientFetch ? new Date(state.lastClientFetch) : new Date(0);
}

async function saveClientAccess(ts) {
  await saveJSON(ACCESS_FILE, { lastClientFetch: ts });
}

function formatDate(date) {
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const yyyy = date.getUTCFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function pseudonymizeName(name) {
  return crypto.createHash("sha256").update(name).digest("hex");
}

/**
 * Authentifie auprès de CellXpert et renvoie le token.
 * Logge uniquement en cas d’échec.
 */
async function authenticate() {
  const qs = new URLSearchParams({
    user: username,
    url: ADMIN_URL,
    pass: password,
  });
  const url = `${CELLXPERT_BASE_URL}?command=authenticate`;

  try {
    const headers = { admin_url: ADMIN_URL, "Content-Type": CONTENT_TYPE };
    const resp = await axios.post(url, qs.toString(), {
      headers,
      timeout: 10000,
    });
    return resp.data.token;
  } catch (err) {
    console.error(
      `[authenticate] POST ${url} failed` +
        ` status=${err.response?.status || "N/A"}` +
        ` message=${err.message}`
    );
    throw err;
  }
}

/**
 * Récupère le rapport de registrations depuis CellXpert.
 * Logge uniquement en cas d’échec.
 */
async function fetchRegistrationReport(token, startDate, endDate) {
  const url = CELLXPERT_BASE_URL;
  const params = {
    command: "processregreport",
    daterange: "registrationdate",
    startDate,
    endDate,
    BTA: true,
    TrackingCode: true,
    QualificationDate: true,
    json: 1,
    ...(AFFILIATE_ID ? { "filter-affiliate": AFFILIATE_ID } : {}),
  };
  const headers = {
    Accept: "application/json",
    Authorization: `Bearer ${token}`,
    admin_url: ADMIN_URL,
  };

  try {
    const resp = await axios.get(url, { params, headers, timeout: 10000 });
    return resp.data.Registrations || [];
  } catch (err) {
    console.error(
      `[fetchRegistrationReport] GET ${url}` +
        ` params=${JSON.stringify(params)}` +
        ` failed status=${err.response?.status || "N/A"}` +
        ` message=${err.message}`
    );
    throw err;
  }
}

function calculateNetDeposit(record) {
  const deposits = record.First_Deposit || 0;
  const pnl = record.PL || 0;
  const withdrawals = record.Withdrawals || 0;

  const usablePnl = Math.max(0, pnl);
  const excess = Math.max(0, withdrawals - usablePnl);

  return deposits - excess;
}

function isRecordChanged(oldRec, newRec) {
  return (
    oldRec.PL !== newRec.PL ||
    oldRec.Withdrawals !== newRec.Withdrawals ||
    oldRec.Commission !== newRec.Commission ||
    oldRec.QualificationDate !== newRec.QualificationDate ||
    oldRec.LotAmount !== newRec.LotAmount
  );
}

/**
 * Récupère l’email pour un userId donné, en round-robin sur les tokens
 * et sous rate-limit Bottleneck. Log uniquement en cas d’erreur.
 * Ne change pas : on récupère l’email en clair.
 */
async function fetchEmail(userId) {
  const cleanId = userId.replace(/^.*?(\d+)$/, "$1");
  console.log(`[fetchEmail] fetching email for userId=${cleanId}`);
  let lastError = null;
  let allRateLimited = true;

  for (let i = 0; i < tokenLimiters.length; i++) {
    const idx = (rrIndex + i) % tokenLimiters.length;
    const { token, limiter } = tokenLimiters[idx];
    const url = `${CUSTOMER_API_URL}?user_id=${cleanId}`;

    try {
      const resp = await limiter.schedule(() =>
        axios.get(url, {
          headers: { "X-API-KEY": process.env.LEAD_EXPORT_API_KEY },
        })
      );
      rrIndex = idx + 1;
      const data = resp.data?.data;
      console.log(`[fetchEmail] GET ${url}  ${resp.data}} tokenIdx=${idx}`);
      if (Array.isArray(data) && data[0]?.email) {
        return data[0].email;
      }
      allRateLimited = false;
      return null;
    } catch (err) {
      lastError = err;
      const status = err.response?.status;
      if (status === 429) continue;
      if (status >= 500) {
        allRateLimited = false;
        continue;
      }
      allRateLimited = false;
      console.error(
        `[fetchEmail] GET ${url} tokenIdx=${idx} failed` +
          ` status=${status || "N/A"} message=${err.message}`
      );
      return null;
    }
  }

  if (allRateLimited && lastError?.response?.status === 429) {
    console.warn(
      `[fetchEmail] all tokens rate-limited for ${userId}, retrying in 60s…`
    );
    await new Promise((r) => setTimeout(r, 60_000));
    return fetchEmail(userId);
  }

  console.error(
    `[fetchEmail] failed for ${userId}` +
      ` message=${lastError?.message || "unknown"}`
  );
  rrIndex = (rrIndex + 1) % tokenLimiters.length;
  return null;
}

function generateMockData(count = 10) {
  const now = new Date().toISOString();
  return Array.from({ length: count }).map((_, i) => {
    const idNum = i + 1;
    const email = `test.user${idNum}@example.com`;

    return {
      CustomerId: `fake_${idNum}`,
      RegistrationDate: now,
      TrackingCode: `TRACK${idNum}`,
      QualificationDate: now,
      LotAmount: parseFloat((Math.random() * 100).toFixed(2)),
      FirstDeposit: parseFloat((Math.random() * 1000).toFixed(2)),
      FirstDepositDate: now,
      NetDeposit: parseFloat((Math.random() * 500).toFixed(2)),
      CustomerNameHash: crypto
        .createHash("sha256")
        .update(`Customer_${idNum}`)
        .digest("hex"),
      Commission: parseFloat((Math.random() * 100).toFixed(2)),
      Email: crypto.createHash("sha256").update(email).digest("hex"),
      modifiedAt: now,
    };
  });
}

/**
 * Récupère, merge et persiste les données + emails (hashés).
 */
async function fetchAndStore() {
  const epoch = new Date("1970-01-01");
  const now = new Date();

  // 1) Auth + rapport brut
  const token = await authenticate();
  const raw = await fetchRegistrationReport(
    token,
    formatDate(epoch),
    formatDate(now)
  );

  // 2) Validation des enregistrements
  const validated = raw
    .map((r) => {
      if (!r.TrackingCode && r.Tracking_Code) r.TrackingCode = r.Tracking_Code;
      const { error, value } = recordSchema.validate(r, { abortEarly: false });
      if (error) {
        console.error(
          `Validation error ${r.User_ID}:`,
          error.details.map((d) => d.message)
        );
        return null;
      }
      return value;
    })
    .filter((r) => r !== null);

  // 3) Chargement du cache d’emails (hashés) existants
  const existing = await loadJSON(DATA_FILE, []);
  const emailCache = new Map(existing.map((r) => [r.CustomerId, r.Email]));

  // 4) Fetch des emails + hashing, avec progression
  const total = validated.length;
  console.log(
    `[fetchAndStore] → début fetchEmail pour ${total} enregistrements`
  );

  await Promise.all(
    validated.map(async (rec, i) => {
      const id = rec.User_ID;
      let hash;

      if (emailCache.has(id)) {
        // hash déjà présent
        hash = emailCache.get(id);
      } else {
        // récupération en clair + hash
        const plain = await fetchEmail(id);
        hash = plain ? pseudonymizeName(plain) : null;
        if (hash) emailCache.set(id, hash);
      }

      // on stocke le hash temporairement
      rec.emailHash = hash;

      // log de progression toutes les 10 et à la fin
      if ((i + 1) % 10 === 0 || i === total - 1) {
        console.log(`[fetchAndStore] … progress: ${i + 1}/${total}`);
      }
    })
  );

  // 5) Fusion avec l’existant et écriture finale
  const mapOld = new Map(existing.map((r) => [r.CustomerId, r]));
  validated.forEach((rec) => {
    const newRec = {
      CustomerId: rec.User_ID,
      RegistrationDate: rec.Registration_Date,
      TrackingCode: rec.TrackingCode,
      QualificationDate: rec.Qualification_Date,
      LotAmount: rec.LOTS ?? 0,
      FirstDeposit: rec.First_Deposit ?? 0,
      FirstDepositDate: rec.First_Deposit_Date,
      NetDeposit:
        calculateNetDeposit({
          ...rec,
          First_Deposit: rec.First_Deposit ?? 0,
          PL: rec.PL ?? 0,
          Withdrawals: rec.Withdrawals ?? 0,
        }) ?? 0,
      CustomerNameHash: pseudonymizeName(rec.Customer_Name),
      Commission: rec.Commissions ?? 0,
      Email: rec.emailHash || null,
    };

    const old = mapOld.get(newRec.CustomerId);
    if (!old || isRecordChanged(old, newRec)) {
      newRec.modifiedAt = now.toISOString();
      mapOld.set(newRec.CustomerId, newRec);
    }
  });

  const merged = Array.from(mapOld.values());
  await saveJSON(DATA_FILE, merged);
  await saveJSON(STATE_FILE, { lastFetch: now.toISOString() });

  console.log(
    `[fetchAndStore] ✔ terminé, ${merged.length} enregistrements enregistrés`
  );
}

async function fetchAndStoreSafe() {
  if (isRunning) {
    console.warn(
      `[skip] Fetch already running, next in ${PULL_INTERVAL_MIN} min`
    );
    return;
  }
  isRunning = true;
  try {
    await fetchAndStore();
  } catch (err) {
    const msg = `[${new Date().toISOString()}] Fetch failed: ${err.message}`;
    console.error(msg);
    await fs.appendFile(ERROR_LOG, `${msg}\n`);
    setTimeout(fetchAndStoreSafe, 60000);
  } finally {
    isRunning = false;
  }
}

// === Express & routes ===
const app = express();
app.use(express.json());

app.use((req, res, next) => {
  const key = req.headers["x-api-key"];
  if (!REPORTS_API_KEY || key !== REPORTS_API_KEY) {
    return res.status(403).json({ error: "Forbidden" });
  }
  next();
});

app.get("/reports", async (req, res) => {
  try {
    const useMock = req.query.mock === "true";
    const dataRaw = useMock
      ? generateMockData(5)
      : await loadJSON(DATA_FILE, []);
    const lastClientFetch = await loadClientAccess();
    const filtered = dataRaw.filter(
      (r) => new Date(r.modifiedAt) > lastClientFetch
    );
    const records = filtered.map((r) => ({
      ...r,
      LotAmount: r.LotAmount ?? 0,
      FirstDeposit: r.FirstDeposit ?? 0,
      NetDeposit: r.NetDeposit ?? 0,
      Withdrawals: r.Withdrawals ?? 0,
      PL: r.PL ?? 0,
      Commission: r.Commission ?? 0,
    }));
    const nowIso = new Date().toISOString();
    await saveClientAccess(nowIso);
    res.set("Last-Modified", nowIso);
    res.json({ count: records.length, records });
  } catch (err) {
    console.error("Error in /reports:", err);
    res.status(500).json({ error: "Unable to load data" });
  }
});

app.get("/reports/full", async (req, res) => {
  try {
    const useMock = req.query.mock === "true";
    const dataRaw = useMock
      ? generateMockData(10)
      : await loadJSON(DATA_FILE, []);
    const records = dataRaw.map((r) => ({
      ...r,
      LotAmount: r.LotAmount ?? 0,
      FirstDeposit: r.FirstDeposit ?? 0,
      NetDeposit: r.NetDeposit ?? 0,
      Withdrawals: r.Withdrawals ?? 0,
      PL: r.PL ?? 0,
      Commission: r.Commission ?? 0,
    }));
    res.set("Last-Modified", new Date().toISOString());
    res.json({ count: records.length, records });
  } catch (err) {
    console.error("Error in /reports/full:", err);
    res.status(500).json({ error: "Unable to load data" });
  }
});

app.get("/health", async (req, res) => {
  try {
    const updateState = await loadJSON(STATE_FILE, {});
    const clientState = await loadJSON(ACCESS_FILE, {});
    res.json({
      status: "ok",
      lastClientFetch: clientState.lastClientFetch || null,
      lastBrokerUpdate: updateState.lastUpdate || updateState.lastFetch || null,
    });
  } catch (err) {
    console.error("Error in /health:", err);
    res.status(500).json({ status: "error", message: err.message });
  }
});

app.get("/meta", async (req, res) => {
  try {
    const data = await loadJSON(DATA_FILE, []);
    const state = await loadJSON(STATE_FILE, {});
    const access = await loadJSON(ACCESS_FILE, {});
    const lastUpdate =
      data.length > 0
        ? new Date(
            Math.max(...data.map((r) => new Date(r.modifiedAt || 0)))
          ).toISOString()
        : null;
    const fileStats = await fs.stat(DATA_FILE);
    const fileSizeKb = (fileStats.size / 1024).toFixed(2);
    res.json({
      version: "1.0.0",
      recordCount: data.length,
      lastBrokerUpdate: state.lastUpdate || state.lastFetch || null,
      lastClientFetch: access.lastClientFetch || null,
      lastUpdateDetected: lastUpdate,
      fileSizeKB: Number(fileSizeKb),
      intervalMinutes: PULL_INTERVAL_MIN,
      affiliateId: AFFILIATE_ID,
      uptimeSeconds: Math.floor((Date.now() - serverStart) / 1000),
      timezone: "UTC",
    });
  } catch (err) {
    console.error("Error in /meta:", err);
    res.status(500).json({ status: "error", message: err.message });
  }
});

// === Démarrage du serveur et du scheduler ===
if (require.main === module) {
  if (!username || !password || !REPORTS_API_KEY || !AFFILIATE_ID) {
    console.error("Missing environment variables: check .env file");
    process.exit(1);
  }

  console.log(`Server starting on port ${PORT}, affiliate ${AFFILIATE_ID}`);
  app.listen(PORT, () => console.log(`API listening on port ${PORT}`));

  // Delay initial fetch to allow DNS/bootstrap
  fetchAndStoreSafe();
  setInterval(fetchAndStoreSafe, PULL_INTERVAL_MIN * 60_000).unref();
}

module.exports = {
  app,
  formatDate,
  pseudonymizeName,
  calculateNetDeposit,
  fetchAndStore,
  fetchRegistrationReport,
};
