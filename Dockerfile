FROM node:24.22.2-alpine AS base
RUN npm install -g pnpm@12.15.0
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

FROM nginx:1.27.3-alpine AS web
COPY --from=build /app/apps/web/dist /usr/share/nginx/html
COPY infra/web-nginx.conf /etc/nginx/conf.d/default.conf
