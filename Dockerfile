FROM node:20-alpine

ENV TZ=Etc/UTC \
    NODE_ENV=production

# Add lightweight process utils
RUN apk add --no-cache bash curl tini

# Install pm2 for multi-process management in Docker
RUN npm i -g pm2@latest

# Create app directory
WORKDIR /usr/src/app

# Install dependencies first (better layer caching)
COPY package*.json ./
RUN npm ci --only=production

# Copy source
COPY . .

# PM2 config
COPY ecosystem.config.cjs ./ecosystem.config.cjs

# Copy env (or mount it at runtime; both work)
# If you prefer mounting, delete the next line.
COPY .env ./.env

# Expose your API server port (server.js uses PORT, default 3002)
EXPOSE 3002

# Use tini as init, then pm2-runtime (Docker-friendly)
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["pm2-runtime", "start", "ecosystem.config.cjs"]
