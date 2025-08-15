FROM node:20-slim

ENV TZ=Etc/UTC \
    NODE_ENV=production

RUN apt-get update && \
    apt-get install -y \
    bash \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g pm2@latest

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["pm2-runtime", "ecosystem.config.cjs"]