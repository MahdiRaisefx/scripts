// algo4.js
// This script syncs lead data from backend to Monday.com board
"use strict";

require("dotenv").config();
const axios = require("axios");
const cron = require("node-cron");
const moment = require("moment");
const BOARD_CONFIG = require("./config/registrationConfig.json");

const MONDAY_API_URL = "https://api.monday.com/v2";
const MONDAY_API_TOKEN = process.env.MONDAY_TOKEN;
const LEAD_EXPORT_API_KEY = process.env.LEAD_EXPORT_API_KEY;
const BACKEND_BASE_URL = process.env.BACKEND_BASE_URL;

// === MONDAY HELPERS ===
async function mondayRequest(query, variables = {}) {
  const res = await axios.post(
    MONDAY_API_URL,
    { query, variables },
    { headers: { Authorization: MONDAY_API_TOKEN } }
  );
  if (res.data.errors) throw new Error(JSON.stringify(res.data.errors));
  return res.data.data;
}

async function getBoardItems(boardId) {
  const query = `
    query ($boardId: [ID!]) {
      boards(ids: $boardId) {
        columns {
          id
          title
          type
        }
        items_page(limit: 500) {
          items {
            id
            name
            column_values {
              id
              text
              value
            }
          }
        }
      }
    }
  `;
  const data = await mondayRequest(query, { boardId });
  const board = data.boards[0];
  // Flatten items_page.items
  return {
    items: board.items_page.items,
    columns: board.columns,
  };
}

async function updateMondayItem(boardId, itemId, columnValues) {
  const { kycCol, statusCol, declinedCol, approvedCol } = columnValues;
  const updateValues = {};

  if (kycCol) {
    const kycStatus = lead.kycPercent || "";
    const kycIndex = mapKycPercentToMondayIndex(kycStatus);
    if (kycIndex !== null) {
      updateValues[kycCol] = { index: kycIndex };
    }
  }

  // Total Declined
  if (declinedCol) {
    updateValues[declinedCol] = lead.totalDeclined || 0;
  }

  if (approvedCol) {
    updateValues[approvedCol] = lead.ftdAmount || 0;
  }

  // Overall Status
  if (statusCol) {
    const statusIndex = mapOverallTypeToMondayIndex(
      lead.kycPercent || "",
      lead.status && lead.status.length
    );
    if (statusIndex !== null) {
      updateValues[statusCol] = { index: statusIndex };
    }
  }

  if (Object.keys(updateValues).length > 0) {
    const query = `
      mutation {
        change_multiple_column_values(
          board_id: ${boardId},
          item_id: ${itemId},
          column_values: "${JSON.stringify(updateValues).replace(/"/g, '\\"')}"
        ) {
          id
        }
      }
    `;
    await mondayRequest(query, {
      itemId: String(itemId),
      columnValues: JSON.stringify(columnValues),
    });
  }
}

async function fetchLeadsByIds(crmIds) {
  const res = await axios.post(`${BACKEND_BASE_URL}/by-ids`, crmIds, {
    headers: { "X-API-KEY": LEAD_EXPORT_API_KEY },
  });
  return res.data;
}

async function syncMondayWithBackend() {
  console.log("🚀 Monday.com sync started");

  // Process each board in the config
  for (const boardConfig of BOARD_CONFIG) {
    console.log(`Processing board: ${boardConfig.name}`);

    try {
      const { items, columns } = await getBoardItems(boardConfig.boardId);

      const crmCol = columns.find(
        (c) => c.title.trim().toLowerCase() === "crm login"
      );
      if (!crmCol) {
        console.log(`CRM Login column not found in board ${boardConfig.name}`);
        continue;
      }

      const mondayCrmIdToItem = {};
      items.forEach((item) => {
        const crmId = item.column_values.find((c) => c.id === crmCol.id)?.text;
        if (crmId) mondayCrmIdToItem[crmId] = item;
      });

      const crmIds = Object.keys(mondayCrmIdToItem).map((id) => Number(id));
      if (crmIds.length === 0) {
        console.log(`No CRM IDs found in Monday board ${boardConfig.name}.`);
        continue;
      }

      const leads = await fetchLeadsByIds(crmIds);

      const kycCol = columns.find((c) => c.title.toLowerCase().includes("kyc"));
      const statusCol = columns.find((c) =>
        c.title.toLowerCase().includes("status")
      );
      const declinedCol = columns.find((c) =>
        c.title.toLowerCase().includes("declined")
      );
      const approvedCol = columns.find((c) =>
        c.title.toLowerCase().includes("ftd amount")
      );

      for (const lead of leads) {
        const item = mondayCrmIdToItem[String(lead.id)];
        if (!item) continue;

        const updateValues = {};
        if (kycCol) updateValues[kycCol.id] = lead.kycPercent || "";
        if (statusCol) updateValues[statusCol.id] = lead.status || "";
        if (declinedCol) updateValues[declinedCol.id] = lead.totalDeclined || 0;
        if (approvedCol) updateValues[approvedCol.id] = lead.ftdAmount || 0;

        console.log("Updating item", item.id, "with values:", updateValues);

        try {
          await updateMondayItem(boardConfig.boardId, item.id, updateValues);
          console.log(
            `✅ Updated CRM ID ${lead.id} in board ${boardConfig.name}`
          );
        } catch (err) {
          if (err.response && err.response.data) {
            console.log("Monday API error:", JSON.stringify(err.response.data));
          }
          console.log(
            `❌ Failed to update CRM ID ${lead.id} in board ${boardConfig.name}: ${err.message}`
          );
        }
      }

      console.log(`✅ Sync completed for board ${boardConfig.name}`);
    } catch (err) {
      console.error(`❌ Error processing board ${boardConfig.name}:`, err);
    }
  }

  console.log("✅ All board syncs completed");
}

// === MAPPING HELPERS ===
function mapOverallTypeToMondayIndex(status, isFresh) {
  if (isFresh) return 7;
  switch ((status || "").toUpperCase()) {
    case "PENDING":
      return 12;
    case "APPROVED":
      return 13;
    case "DENIED":
      return 3;
    default:
      return null;
  }
}

function mapKycPercentToMondayIndex(status) {
  switch ((status || "").toUpperCase()) {
    case "APPROVED":
      return 1;
    case "DENIED":
      return 0;
    case "PENDING":
      return 2;
    default:
      return 2;
  }
}

// === SCHEDULING ===
console.log("🚀 Monday.com sync script started");

// Run every 15 minutes
cron.schedule("*/15 * * * *", () => {
  console.log(`🕒 [${moment().format()}] Running scheduled sync`);
  syncMondayWithBackend();
});

// Initial run
(async () => {
  console.log("🚀 Initial sync at script startup");
  await syncMondayWithBackend();
})();
