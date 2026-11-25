# Node 24 runs TypeScript directly (type stripping), so the image needs no build step.
FROM node:24-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src
COPY sim ./sim

ENV NODE_ENV=production
EXPOSE 8080
USER node

CMD ["node", "src/server.ts"]
