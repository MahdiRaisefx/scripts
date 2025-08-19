const axios = require("axios");
const cron = require("node-cron");
require("dotenv").config();
const BOARD_CONFIG = require("./config/registrationConfig.json");

const MONDAY_API_URL = "https://api.monday.com/v2";
const MONDAY_API_TOKEN = process.env.MONDAY_TOKEN;
const LEAD_EXPORT_API_KEY = process.env.LEAD_EXPORT_API_KEY;
const BACKEND_BASE_URL = process.env.BACKEND_BASE_URL;

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

async function createMondayLeadItem(lead, boardState, userId, boardId) {
  const displayName = getNameFromLead(lead);
  console.log(`Extracted display name: ${displayName}`);

  const kycIndex = getKycStatusIndex(lead.kycPercent, boardState);

  // Build column values object with proper formatting
  const columnValues = {
    [boardState.crmCol]: userId,
    [boardState.nameCol]: displayName,
    [boardState.emailCol]: lead.email
      ? JSON.stringify({ email: lead.email, text: lead.email })
      : null,
    [boardState.phoneCol]: lead.phone
      ? JSON.stringify({
          phone: String(lead.phone),
          countryShortName: lead.countryCode || "US",
        })
      : null,
    [boardState.birthDateCol]: lead.birthDate
      ? JSON.stringify({
          date: new Date(lead.birthDate).toISOString().split("T")[0],
        })
      : null,
    [boardState.addressCol]: lead.address || null,
    [boardState.countryCol]: lead.country || null,
    [boardState.regDateCol]: lead.registrationDate
      ? JSON.stringify({
          date: new Date(lead.registrationDate).toISOString().split("T")[0],
        })
      : null,
    [boardState.kycCol]: JSON.stringify({ index: kycIndex }),
    [boardState.totalDeclinedCol]: lead.totalDeclined || null,
    [boardState.ftdAmountCol]: lead.ftdAmount || null,
    [boardState.ftdDateCol]: lead.ftdDate
      ? JSON.stringify({
          date: new Date(lead.ftdDate).toISOString().split("T")[0],
        })
      : null,
  };

  // Filter out null values
  const filteredValues = Object.fromEntries(
    Object.entries(columnValues).filter(([_, v]) => v !== null)
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
    const resp = await axios.get(`${BACKEND_BASE_URL}/all`, {
      headers: {
        "X-API-KEY": LEAD_EXPORT_API_KEY || "",
      },
    });

    const leads = resp.data;
    const now = Date.now();

    const assignBoard = await getMondayBoardState(NEW_LEADS_BOARD);
    const ncSelfBoard = await getMondayBoardState(NC_SALES_BOARD);

    const assignCrmSet = new Set(
      assignBoard.items
        .map(
          (item) =>
            item.column_values.find((c) => c.id === assignBoard.crmCol)?.text
        )
        .filter((text) => text !== undefined && text !== null)
    );

    const ncSelfCrmSet = new Set(
      ncSelfBoard.items
        .map(
          (item) =>
            item.column_values.find((c) => c.id === ncSelfBoard.crmCol)?.text
        )
        .filter((text) => text !== undefined && text !== null)
    );

    for (const lead of leads) {
      const userId = String(lead.id);
      const regTime = new Date(lead.registrationDate).getTime();

      if (!isNaN(regTime) && now - regTime < 2 * 60 * 60 * 1000) continue;
      if (assignCrmSet.has(userId) || ncSelfCrmSet.has(userId)) continue;

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
        colorLog(
          "success",
          `✅ Added user ${userId} (${
            lead.crmLogin || lead.email || "-"
          }) to '${boardName}'`
        );
      } catch (err) {
        colorLog("error", `❌ Failed to add user ${userId}: ${err.message}`);
      }
    }

    colorLog("info", "✅ Sync completed successfully");
  } catch (err) {
    colorLog("error", `❌ Error: ${err}`);
  } finally {
    isRunning = false;
  }
}

// ---- Schedule ----
cron.schedule("*/15 * * * *", processCustomers);
console.log("🚀 Scheduler started: every 15 minutes");
processCustomers();
