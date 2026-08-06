FROM node:20-alpine
WORKDIR /suite
RUN npm install --no-fund --no-audit ioredis@5
COPY compat.mjs /suite/compat.mjs
CMD ["node", "/suite/compat.mjs"]
