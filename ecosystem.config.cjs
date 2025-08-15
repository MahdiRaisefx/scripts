module.exports = {
  apps: [
    {
      name: "server",
      script: "server.js",
      env: {
        NODE_ENV: "production",
      },
    },

    {
      name: "registration_tracker",
      script: "registration_tracker.js",
      env: { NODE_ENV: "production" },
    },
    {
      name: "registration_updater",
      script: "registration_updater.js",
      env: { NODE_ENV: "production" },
    },
    {
      name: "retention_updater",
      script: "retention_updater.js",
      env: { NODE_ENV: "production" },
    },
    {
      name: "sales_updater",
      script: "sales_updater.js",
      env: { NODE_ENV: "production" },
    },
  ],

  deploy: {},
};
