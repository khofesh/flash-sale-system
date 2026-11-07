FROM node:26.13.0-alpine3.24 AS base
# npm is only needed to install pnpm; its bundled deps carry CVEs, so drop it.
RUN npm install -g pnpm@12.18.0 \
 && rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx
WORKDIR /app

FROM base AS deps
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml ./
COPY apps/api/package.json apps/api/package.json
COPY apps/orchestrator/package.json apps/orchestrator/package.json
COPY apps/payment/package.json apps/payment/package.json
COPY apps/shipping/package.json apps/shipping/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/config/package.json packages/config/package.json
COPY packages/core/package.json packages/core/package.json
COPY packages/infra/package.json packages/infra/package.json
RUN pnpm install --frozen-lockfile

FROM deps AS build
ARG APP
COPY . .
RUN pnpm --filter "@fss/${APP}..." build

FROM base AS service
ARG APP
ENV NODE_ENV=production
COPY --from=build /app /app
ENV APP=${APP}
CMD ["sh", "-c", "node apps/${APP}/dist/index.js"]

FROM nginx:1.30.6-alpine3.24 AS web
COPY --from=build /app/apps/web/dist /usr/share/nginx/html
COPY infra/web-nginx.conf /etc/nginx/conf.d/default.conf
