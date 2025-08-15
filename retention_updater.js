"use strict";

const axios = require("axios");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const MONDAY_API_TOKEN = process.env.Monday_Token;

// === CONFIGURATION ===
const retentionConfigPath = "./config/retentionConfig.json";
const LeadExportBaseUrl = process.env.BACKEND_BASE_URL;
const LeadExportApiKey = process.env.LEAD_EXPORT_API_KEY;
const MONDAY_API_URL = "https://api.monday.com/v2";
const STATE_FILE = path.join(__dirname, "sync_state.json");

// --- State Management ---
let state = {};
if (fs.existsSync(STATE_FILE)) {
  state = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
}

function saveState() {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// --- Monday.com API Functions ---
async function mondayRequest(query, variables = {}) {
  try {
    const res = await axios.post(
      MONDAY_API_URL,
      { query, variables },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: MONDAY_API_TOKEN,
        },
      }
    );
    if (res.data.errors) {
      console.error(
        "🔴 GraphQL Errors:",
        JSON.stringify(res.data.errors, null, 2)
      );
      throw new Error("GraphQL returned errors");
    }
    return res.data.data;
  } catch (err) {
    if (err.response) {
      console.error(
        "❌ Monday API HTTP Error:",
        err.response.status,
        JSON.stringify(err.response.data, null, 2)
      );
    } else {
      console.error("❌ Monday request failed (network error):", err.message);
    }
    throw err;
  }
}

async function fetchColumns(boardId) {
  const query = `query { boards(ids: [${boardId}]) { columns { id title type } } }`;
  const data = await mondayRequest(query);
  return data.boards[0].columns;
}

async function fetchItems(boardId) {
  const query = `query {
    boards(ids: [${boardId}]) {
      items_page(limit: 500) {
        items {
          id
          name
          group { id title }
          column_values { id text }
        }
      }
    }
  }`;
  const data = await mondayRequest(query);
  return data.boards[0].items_page.items;
}

async function updateItem(boardId, itemId, updates) {
  const mutation = `mutation ($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
    change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
  }`;
  for (const { columnId, value } of updates) {
    const formattedValue = JSON.stringify(String(value)); // Monday expects a JSON string
    await mondayRequest(mutation, {
      itemId,
      boardId,
      columnId,
      value: formattedValue,
    });
  }
}

async function createTransactionItem(boardId, itemName, columnMap, data) {
  const mutation = `mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
    create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) { id }
  }`;
  const variables = {
    boardId: boardId.toString(),
    itemName,
    columnValues: JSON.stringify({
      [columnMap["Login CRM"]]: String(data.userId),
      [columnMap["Deposit Amount"]]: data.totalDeposit.toFixed(2),
      [columnMap["Withdrawal Amount"]]: data.totalWD.toFixed(2),
      [columnMap["Transaction Date"]]: {
        date: new Date().toISOString().split("T")[0],
      },
    }),
  };
  await mondayRequest(mutation, variables);
}

// --- Backend API Function ---

async function getTransactionTotalsFromBackend(clients) {
  if (clients.length === 0) {
    console.log("ℹ️ No clients to process.");
    return [];
  }

  const formattedClients = clients.map((client) => ({
    crm_id: client.crm_id,
    since: new Date(client.since).toISOString(), // e.g., 2025-08-14T12:34:56.789Z
  }));

  try {
    const response = await axios.post(
      LeadExportBaseUrl + "/totals",
      { clients: formattedClients },
      {
        headers: { "X-API-KEY": LeadExportApiKey },
      }
    );

    console.log("Response data:", response.data);
    return response.data;
  } catch (error) {
    if (error.response) {
      console.error("Backend responded with error:", {
        status: error.response.status,
        data: error.response.data,
      });
    } else if (error.request) {
      console.error("No response received:", error.message);
    } else {
      console.error("Request setup error:", error.message);
    }
    return [];
  }
}

// --- Main Processing Function ---
async function processBoard({ name, boardId, transactionBoardId }) {
  console.log(`\n🔄 Starting sync for ${name}`);
  const excludedGroups = [
    "Fresh clients",
    "No Answer",
    "Client Incative/Don't want contact/Dead Lead/Never Answered",
  ];

  const items = await fetchItems(boardId);
  const columns = await fetchColumns(boardId);
  const txColumns = await fetchColumns(transactionBoardId);

  const colMap = Object.fromEntries(columns.map((c) => [c.title, c.id]));
  const txColMap = Object.fromEntries(txColumns.map((c) => [c.title, c.id]));

  const lastCheck = state[name]?.lastCheck;
  console.log(`ℹ️ Last check for ${name}: ${lastCheck || "Never (first run)"}`);
  const now = new Date().toISOString();

  // 1. COLLECT all client data first
  const clientsForApiCall = [];

  for (const item of items) {
    if (excludedGroups.includes(item.group.title)) continue;

    const crmLoginCol = item.column_values.find(
      (c) => c.id === colMap["CRM Login"]
    );
    const crmLogin = crmLoginCol?.text;

    if (!crmLogin || isNaN(Number(crmLogin))) continue;

    const retentionDateCol = item.column_values.find(
      (c) => c.id === colMap["Retention Assigned"]
    );

    // Determine 'since' (as an ISO moment with Z). We'll strip the offset later for the backend.
    let sinceDateIsoZ;
    if (lastCheck) {
      // last run time already in ISO with Z
      sinceDateIsoZ = lastCheck;
    } else if (retentionDateCol?.text) {
      // Monday date columns are typically 'YYYY-MM-DD'. Assume start of day UTC.
      // Safer than new Date('YYYY-MM-DDZ') which is not guaranteed to parse in all runtimes.
      sinceDateIsoZ = `${retentionDateCol.text}T00:00:00Z`;
    } else {
      sinceDateIsoZ = "1970-01-01T00:00:00Z";
    }

    clientsForApiCall.push({
      crm_id: Number(crmLogin),
      since: sinceDateIsoZ,
    });
  }

  // Add debug logging to verify the request payload (first 3)
  console.log(
    "Sending clients data (sample):",
    JSON.stringify(
      {
        clients: clientsForApiCall.slice(0, 3),
      },
      null,
      2
    )
  );

  // 2. MAKE A SINGLE API call to the backend
  const transactionData = await getTransactionTotalsFromBackend(
    clientsForApiCall
  );
  const totalsMap = new Map(transactionData.map((t) => [t.userId, t]));

  // 3. PROCESS results and update Monday.com
  for (const item of items) {
    const crmLoginCol = item.column_values.find(
      (c) => c.id === colMap["CRM Login"]
    );
    const crmLogin = crmLoginCol?.text;
    if (!crmLogin) continue;

    const userTotals = totalsMap.get(Number(crmLogin));

    if (userTotals) {
      console.log(`Updating data for CRM: ${crmLogin}`);

      // A. Update the main retention board with the new totals
      const updates = [
        { columnId: colMap["Total Deposit"], value: userTotals.totalDeposit },
        { columnId: colMap["Total WD"], value: userTotals.totalWD },
        {
          columnId: colMap["Total Deposit Declined"],
          value: userTotals.totalDepositDeclined,
        },
        {
          columnId: colMap["Total WD Declined"],
          value: userTotals.totalWDDeclined,
        },
      ];
      await updateItem(boardId, item.id, updates);

      // B. Log the new transaction totals in the transaction board
      if (userTotals.totalDeposit > 0 || userTotals.totalWD > 0) {
        await createTransactionItem(transactionBoardId, item.name, txColMap, {
          userId: userTotals.userId,
          totalDeposit: userTotals.totalDeposit,
          totalWD: userTotals.totalWD,
        });
      }
    }
  }

  // 4. SAVE the state for the next run
  state[name] = { lastCheck: now };
  saveState();
  console.log(`✅ Sync complete for ${name}. Next check will be after ${now}`);
}

// --- Main Execution ---
async function main() {
  const retentionConfig = JSON.parse(
    fs.readFileSync(retentionConfigPath, "utf-8")
  );

  for (const cfg of retentionConfig) {
    try {
      await processBoard(cfg);
    } catch (err) {
      console.error(
        `❌❌ A critical error occurred during the sync for ${cfg.name}:`,
        err.message
      );
    }
  }
}

// Run the script immediately and then every 30 minutes
main();
setInterval(main, 30 * 60 * 1000);
