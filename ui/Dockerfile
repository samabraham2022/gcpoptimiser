# Stage 1: Build React static assets
FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Stage 2: Express BFF Server
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY server.js .
COPY kafka-piler.js .
COPY kafka-microservice-consumer.js .
COPY --from=builder /app/build ./build

EXPOSE 3000
CMD ["node", "server.js"]