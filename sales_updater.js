// sales_updater.js
// This script updates FTD AMOUNT/Challenge in Monday.com sales boards from backend data

require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const cron = require("node-cron");
const MONDAY_API_URL = "https://api.monday.com/v2";
const LeadExportBaseUrl = process.env.BACKEND_BASE_URL;
const LeadExportApiKey = process.env.LEAD_EXPORT_API_KEY;
const MONDAY_API_TOKEN = process.env.Monday_Token;
const SALES_CONFIG_PATH = "./config/salesUpdaterConfig.json";

async function mondayRequest(query, variables = {}) {
  const res = await axios.post(
    MONDAY_API_URL,
    { query, variables },
    {
      headers: { Authorization: MONDAY_API_TOKEN },
    }
  );
  if (res.data.errors) throw new Error(JSON.stringify(res.data.errors));
  return res.data.data;
}

async function getBoardItems(boardId) {
  const query = `
    query ($boardId: [ID!]) {
      boards(ids: $boardId) {
        columns { id title }
        items_page(limit: 500) {
          items { id name column_values { id text value } }
        }
      }
    }
  `;
  const data = await mondayRequest(query, { boardId });
  const board = data.boards[0];
  return { items: board.items_page.items, columns: board.columns };
}

async function updateMondayItem(boardId, itemId, columnId, value) {
  const query = `
    mutation {
      change_column_value(
        board_id: ${boardId},
        item_id: ${itemId},
        column_id: "${columnId}",
        value: "${value}"
      ) { id }
    }
  `;
  await mondayRequest(query);
}

async function fetchTransactionsByUserIds(userIds) {
  const res = await axios.post(
    LeadExportBaseUrl + "/transactions-by-user-ids",
    userIds,
    {
      headers: { "X-API-KEY": LeadExportApiKey },
    }
  );
  return res.data;
}

async function updateSalesData() {
  try {
    console.log("🚀 Starting sales data update...");
    const salesConfig = JSON.parse(fs.readFileSync(SALES_CONFIG_PATH, "utf-8"));

    for (const { name, boardId } of salesConfig) {
      console.log(`Processing board for ${name} (ID: ${boardId})`);
      const { items, columns } = await getBoardItems(boardId);

      const crmCol = columns.find(
        (c) => c.title.trim().toLowerCase() === "crm login"
      );
      const ftdCol = columns.find(
        (c) => c.title.trim().toLowerCase() === "ftd amount/challenge"
      );

      if (!crmCol || !ftdCol) {
        console.error(`Missing columns in board ${boardId}`);
        continue;
      }

      const crmIdToItem = {};
      items.forEach((item) => {
        const crmId = item.column_values.find((c) => c.id === crmCol.id)?.text;
        if (crmId) crmIdToItem[crmId] = item;
      });

      const crmIds = Object.keys(crmIdToItem).map((id) => Number(id));
      if (crmIds.length === 0) continue;

      // Fetch transactions for all userIds
      const transactions = await fetchTransactionsByUserIds(crmIds);

      for (const tx of transactions) {
        if (!tx || !tx.user || !tx.user.id) continue;
        const item = crmIdToItem[String(tx.user.id)];
        if (!item) continue;

        const usdAmount = tx.amount || 0;
        await updateMondayItem(boardId, item.id, ftdCol.id, usdAmount);
        console.log(
          `Updated CRM ID ${tx.user.id} in board ${boardId} with FTD AMOUNT/Challenge: ${usdAmount}`
        );
      }
    }
    console.log("✅ Sales data update completed successfully");
  } catch (error) {
    console.error("❌ Error during sales data update:", error.message);
  }
}

// Schedule to run every 15 minutes
console.log("⏰ Scheduling sales data updates every 15 minutes...");
cron.schedule("*/15 * * * *", updateSalesData);

// Initial run
updateSalesData();
