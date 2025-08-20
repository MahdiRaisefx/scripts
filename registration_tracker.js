const axios = require("axios");
const cron = require("node-cron");
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const xml2js = require("xml2js");
const BOARD_CONFIG = require("./config/registrationConfig.json");
let affiliateMap = {};

const MONDAY_API_URL = "https://api.monday.com/v2";
const MONDAY_API_TOKEN = process.env.MONDAY_TOKEN;
const LEAD_EXPORT_API_KEY = process.env.LEAD_EXPORT_API_KEY;
const BACKEND_BASE_URL = process.env.BACKEND_BASE_URL;

const PARTNERS_BASE = "https://partners.raisefx.com/api/admin";
const PARTNERS_AUTH = {
  api_username: process.env.API_USERNAME,
  api_password: process.env.API_PASSWORD,
};

// File to track last processed registration date
const LAST_PROCESSED_FILE = path.join(__dirname, "last_processed.json");

// Find board IDs from config
const NEW_LEADS_BOARD = BOARD_CONFIG.find(
  (b) => b.name === "New Leads"
)?.boardId;
console.log(`New Leads Board ID: ${NEW_LEADS_BOARD}`);
if (!NEW_LEADS_BOARD) {
  console.error("New Leads board ID not found.");
}
const NC_SALES_BOARD = BOARD_CONFIG.find((b) => b.name === "Nc self")?.boardId;

console.log(`Nc self Board ID: ${NC_SALES_BOARD}`);
if (!NC_SALES_BOARD) {
  console.error("Nc self board ID not found.");
}

// Load last processed date
function loadLastProcessedDate() {
  try {
    if (fs.existsSync(LAST_PROCESSED_FILE)) {
      const data = fs.readFileSync(LAST_PROCESSED_FILE, "utf8");
      return JSON.parse(data).lastProcessedDate;
    }
  } catch (error) {
    console.log("warn", `Failed to load last processed date: ${error.message}`);
  }
  return null;
}

// Save last processed date
function saveLastProcessedDate(date) {
  try {
    const data = JSON.stringify({ lastProcessedDate: date });
    fs.writeFileSync(LAST_PROCESSED_FILE, data);
  } catch (error) {
    console.log(
      "error",
      `Failed to save last processed date: ${error.message}`
    );
  }
}

async function fetchAffiliateList() {
  const url = `${PARTNERS_BASE}/?api_username=${encodeURIComponent(
    PARTNERS_AUTH.api_username
  )}&api_password=${encodeURIComponent(
    PARTNERS_AUTH.api_password
  )}&command=affiliatelist&json=1`;
  try {
    const res = await axios.get(url, { timeout: 10000 });
    const list = res.data || [];
    affiliateMap = Object.fromEntries(
      list.map((a) => [
        String(a.AffiliateID),
        `${a.FirstName} ${a.LastName}`.trim(),
      ])
    );
    console.log(
      "info",
      `Loaded ${Object.keys(affiliateMap).length} affiliates`
    );
  } catch (err) {
    console.log("error", `Fetch affiliates: ${err.message}`);
    throw err;
  }
}

// Color log helper
function colorLog(level, msg) {
  const ts = new Date().toISOString();
  let color = "";

  switch (level) {
    case "info":
      color = "\x1b[36m";
      break;
    case "success":
      color = "\x1b[32m";
      break;
    case "warn":
      color = "\x1b[33m";
      break;
    case "error":
      color = "\x1b[31m";
      break;
    default:
      color = "\x1b[0m";
      break;
  }

  console.log(
    `${color}[${ts}] [${level.toUpperCase().padEnd(7)}] ${msg}\x1b[0m`
  );
}

// ---- Monday helpers ----
async function mondayRequest(query, variables = {}) {
  const res = await axios.post(
    MONDAY_API_URL,
    { query, variables },
    {
      headers: {
        Authorization: MONDAY_API_TOKEN,
        "Content-Type": "application/json",
      },
    }
  );
  if (res.data.errors) throw new Error(JSON.stringify(res.data.errors));
  return res.data.data;
}

async function getMondayBoardState(boardId) {
  const allItems = [];
  let cursor = null;
  let columns = [];

  const query = `
    query ($boardId: [ID!], $cursor: String) {
      boards(ids: $boardId) {
        columns { id title type settings_str }
        items_page(limit: 500, cursor: $cursor) {
          cursor
          items {
            id
            name
            column_values { id text value }
          }
        }
      }
    }
  `;

  do {
    const data = await mondayRequest(query, { boardId, cursor });
    const board = data.boards[0];
    columns = board.columns;
    const page = board.items_page;
    allItems.push(...page.items);
    cursor = page.cursor;
  } while (cursor);

  // Map all important columns
  const columnMap = {
    crmCol: columns.find((c) => c.title === "CRM Login")?.id,
    nameCol: columns.find((c) => c.title === "Name")?.id,
    emailCol: columns.find((c) => c.title === "Email")?.id,
    phoneCol: columns.find((c) => c.title === "Phone")?.id,
    birthDateCol: columns.find((c) => c.title === "Date of Birth")?.id,
    addressCol: columns.find((c) => c.title === "Adress")?.id,
    countryCol: columns.find((c) => c.title === "Country")?.id,
    regDateCol: columns.find((c) => c.title === "Registration Date")?.id,
    kycCol: columns.find((c) => c.title === "KYC %")?.id,
    totalDeclinedCol: columns.find((c) => c.title === "Total Declined")?.id,
    ftdAmountCol: columns.find((c) => c.title === "FTD AMOUNT/Challenge")?.id,
    ftdDateCol: columns.find((c) => c.title === "FTD/Challenge Date")?.id,
    affiliateNameCol: columns.find((c) => c.title === "Affiliate Name")?.id,
    triedDepositCol: columns.find((c) => c.title === "Tried Deposit")?.id,
  };

  return { boardId, items: allItems, columns, ...columnMap };
}

// ---- KYC mapping (auto-detect status index) ----
function norm(s) {
  return (s || "").toString().toLowerCase();
}

function parseStatusLabels(settingsStr) {
  if (!settingsStr) return {};

  try {
    const settings = JSON.parse(settingsStr);
    const labels =
      settings.labels ||
      settings?.labels_positions ||
      settings?.labels_text ||
      null;
    const indexToLabel = {};

    if (Array.isArray(labels)) {
      labels.forEach((label, idx) => {
        if (label) indexToLabel[idx] = String(label);
      });
    } else if (labels && typeof labels === "object") {
      Object.keys(labels).forEach((k) => {
        const idx = Number(k);
        if (!Number.isNaN(idx) && labels[k])
          indexToLabel[idx] = String(labels[k]);
      });
    }
    return indexToLabel;
  } catch {
    return {};
  }
}

function bucketFromKycValue(v) {
  const val = norm(v);
  const approved = [
    "approved",
    "approved_with_mismatch",
    "completed",
    "verified",
    "success",
    "ok",
    "pass",
  ];
  const pending = [
    "pending",
    "awaiting_self_review",
    "awaiting_kyc_process",
    "review",
    "processing",
    "in progress",
  ];
  const denied = [
    "denied",
    "expired",
    "suspected",
    "rejected",
    "failed",
    "blocked",
  ];

  if (approved.some((k) => val.includes(k))) return "APPROVED";
  if (denied.some((k) => val.includes(k))) return "DENIED";
  if (pending.some((k) => val.includes(k))) return "PENDING";
  return "PENDING";
}

function pickIndexForBucket(indexToLabel, bucket) {
  const scores = Object.entries(indexToLabel).map(([idx, label]) => {
    const L = norm(label);
    let s = 0;

    if (L.includes("0%")) s += bucket === "DENIED" ? 3 : 0;
    if (L.includes("50%")) s += bucket === "PENDING" ? 3 : 0;
    if (L.includes("100%")) s += bucket === "APPROVED" ? 3 : 0;

    if (bucket === "APPROVED") {
      if (/(approved|complete|verified|success|ok|pass)/.test(L)) s += 5;
      if (/(green)/.test(L)) s += 1;
    } else if (bucket === "PENDING") {
      if (/(pending|review|await|processing|in\s*progress)/.test(L)) s += 5;
      if (/(yellow|orange)/.test(L)) s += 1;
    } else if (bucket === "DENIED") {
      if (/(denied|rejected|expired|failed|blocked)/.test(L)) s += 5;
      if (/(red)/.test(L)) s += 1;
    }

    return { idx: Number(idx), score: s };
  });

  scores.sort((a, b) => b.score - a.score);
  if (scores.length && scores[0].score > 0) return scores[0].idx;

  if (bucket === "APPROVED" && 2 in indexToLabel) return 2;
  if (bucket === "PENDING" && 1 in indexToLabel) return 1;
  if (bucket === "DENIED" && 0 in indexToLabel) return 0;

  const first = Object.keys(indexToLabel)
    .map(Number)
    .sort((a, b) => a - b)[0];
  return Number.isFinite(first) ? first : 0;
}

function getKycStatusIndex(kycRawValue, boardState) {
  const kycColId = boardState.kycCol;
  if (!kycColId) return 0;

  const kycCol = boardState.columns.find((c) => c.id === kycColId);
  const indexToLabel = parseStatusLabels(kycCol?.settings_str);
  const bucket = bucketFromKycValue(kycRawValue);

  return pickIndexForBucket(indexToLabel, bucket);
}

// ---- Formatting helpers ----
function getNameFromLead(lead) {
  if (lead.fullName && lead.fullName.trim().length > 0) {
    return lead.fullName.trim();
  }

  const emailCandidate =
    lead.email && lead.email.includes("@")
      ? lead.email
      : lead.crmLogin && lead.crmLogin.includes("@")
      ? lead.crmLogin
      : null;

  if (emailCandidate) {
    const local = emailCandidate.split("@")[0] || "";
    let namePart = local
      .replace(/^[0-9._+-]+/, "")
      .replace(/[._+-]+$/, "")
      .replace(/[._+-]+/g, " ")
      .trim();

    namePart = namePart
      .split(" ")
      .filter(Boolean)
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
      .join(" ");

    return namePart || String(lead.id);
  }

  return String(lead.id);
}

function formatColumnValue(value, columnType) {
  if (value === undefined || value === null) return null;

  switch (columnType) {
    case "email":
      return { email: value, text: value };
    case "date": {
      const d = new Date(value);
      if (isNaN(d.getTime())) return null;
      return { date: d.toISOString().split("T")[0] };
    }
    case "status":
      return value;
    case "numbers":
      return String(value);
    default:
      return String(value);
  }
}

async function fetchRegistration(userId) {
  const url = `${PARTNERS_BASE}/?api_username=${encodeURIComponent(
    PARTNERS_AUTH.api_username
  )}&api_password=${encodeURIComponent(
    PARTNERS_AUTH.api_password
  )}&command=registrations&userid=raisefx-${userId}`;
  try {
    const xml = (await axios.get(url)).data;
    const trimmed = xml.trim();
    if (!trimmed || trimmed === "<></>") return {};
    const parsed = await new xml2js.Parser({
      explicitArray: false,
      explicitRoot: false,
    }).parseStringPromise(trimmed);
    return parsed.row || {};
  } catch (err) {
    console.log("error", `Fetch reg ${userId}: ${err.message}`);
    return {};
  }
}

async function createMondayLeadItem(lead, boardState, userId, boardId) {
  const displayName = getNameFromLead(lead);
  console.log(`Extracted display name: ${displayName}`);
  await fetchAffiliateList();
  let formattedPhone = null;
  if (lead.phone) {
    formattedPhone = lead.phone.replace(/\s+/g, "");
    console.log(
      `Original phone: ${lead.phone}, Formatted phone: ${formattedPhone}`
    );
  }
  const kycIndex = getKycStatusIndex(lead.kycPercent, boardState);
  const reg = await fetchRegistration(lead.id);
  const id = reg.affiliateID || "";
  const name = affiliateMap[id] || `ID ${id}`;
  const label = id ? `${name} (${id})` : "";
  const columnValues = {
    [boardState.crmCol]: userId,
    [boardState.nameCol]: displayName,
    [boardState.emailCol]: formatColumnValue(lead.email, "email"),
    [boardState.phoneCol]: {
      phone: formattedPhone,
      countryShortName: lead.country || "US",
    },
    [boardState.birthDateCol]: formatColumnValue(lead.birthDate, "date"),
    [boardState.addressCol]: lead.address ?? null,
    [boardState.countryCol]: lead.country ?? null,
    [boardState.regDateCol]: formatColumnValue(lead.registrationDate, "date"),
    [boardState.kycCol]: formatColumnValue({ index: kycIndex }, "status"),
    [boardState.totalDeclinedCol]: lead.totalDeclined,
    [boardState.ftdAmountCol]: lead.ftdAmount,
    [boardState.ftdDateCol]: formatColumnValue(lead.ftdDate, "date"),
    [boardState.affiliateNameCol]: label,
    [boardState.triedDepositCol]: lead.triedDeposit,
  };

  const filteredValues = Object.fromEntries(
    Object.entries(columnValues).filter(
      ([_, v]) => v !== null && v !== undefined
    )
  );

  const mutation = `
    mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
      create_item(
        board_id: $boardId,
        item_name: $itemName,
        column_values: $columnValues
      ) { id }
    }
  `;

  await mondayRequest(mutation, {
    boardId,
    itemName: displayName,
    columnValues: JSON.stringify(filteredValues),
  });
}

// ---- Main sync job ----
let isRunning = false;

async function processCustomers() {
  if (isRunning) return;
  isRunning = true;

  try {
    const lastProcessedDate = loadLastProcessedDate();
    const sinceParam = lastProcessedDate ? `?since=${lastProcessedDate}` : "";

    const resp = await axios.get(`${BACKEND_BASE_URL}/all${sinceParam}`, {
      headers: {
        "X-API-KEY": LEAD_EXPORT_API_KEY || "",
      },
    });

    const leads = resp.data;

    if (leads.length === 0) {
      colorLog("info", "No new registrations since last check");
      return;
    }

    const assignBoard = await getMondayBoardState(NEW_LEADS_BOARD);
    const ncSelfBoard = await getMondayBoardState(NC_SALES_BOARD);

    // Get all existing user IDs from both boards to avoid duplicates
    const existingUserIds = new Set();

    [assignBoard, ncSelfBoard].forEach((board) => {
      board.items.forEach((item) => {
        const userId = item.column_values.find(
          (c) => c.id === board.crmCol
        )?.text;
        if (userId) {
          existingUserIds.add(userId);
        }
      });
    });

    let latestDate = lastProcessedDate;
    let processedCount = 0;

    for (const lead of leads) {
      const userId = String(lead.id);

      if (existingUserIds.has(userId)) {
        colorLog("info", `⏩ Skipping existing user ${userId}`);
        continue;
      }

      const hasDeposited =
        Boolean(lead.triedDeposit) ||
        (lead.ftdAmount && Number(lead.ftdAmount) > 0);
      const targetBoard = hasDeposited ? ncSelfBoard : assignBoard;
      const boardName = hasDeposited ? "Nc sales" : "New Leads";

      try {
        await createMondayLeadItem(
          lead,
          targetBoard,
          userId,
          targetBoard.boardId
        );
        console.log("lead affiliate name:", lead.affiliateName);
        colorLog(
          "success",
          `✅ Added user ${userId} (${
            lead.crmLogin || lead.email || "-"
          }) to '${boardName}'`
        );
        processedCount++;

        // Update latest date
        if (
          lead.registrationDate &&
          (!latestDate ||
            new Date(lead.registrationDate) > new Date(latestDate))
        ) {
          latestDate = lead.registrationDate;
        }
      } catch (err) {
        colorLog("error", `❌ Failed to add user ${userId}: ${err.message}`);
      }
    }

    // Save the latest processed date
    if (latestDate && latestDate !== lastProcessedDate) {
      saveLastProcessedDate(latestDate);
      colorLog("info", `📅 Updated last processed date to: ${latestDate}`);
    }

    colorLog(
      "info",
      `✅ Sync completed. Processed ${processedCount} new users`
    );
  } catch (err) {
    colorLog("error", `❌ Error: ${err.message}`);
  } finally {
    isRunning = false;
  }
}

// ---- Schedule ----
cron.schedule("*/15 * * * *", processCustomers);
console.log("🚀 Scheduler started: every 15 minutes");

// Initial run
processCustomers();
