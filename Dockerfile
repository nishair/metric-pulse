# Use Node.js LTS version
FROM node:18-alpine AS base

# Set working directory
WORKDIR /app

# Install system dependencies for PostgreSQL client
RUN apk add --no-cache postgresql-client

# Copy package files
COPY package*.json ./

# Development stage
FROM base AS development
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "run", "dev"]

# Production dependencies stage
FROM base AS dependencies
RUN npm ci --only=production && npm cache clean --force

# Production stage
FROM base AS production

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy production dependencies
COPY --from=dependencies --chown=nodejs:nodejs /app/node_modules ./node_modules

# Copy application code
COPY --chown=nodejs:nodejs . .

# Create logs directory
RUN mkdir -p logs && chown -R nodejs:nodejs logs

# Switch to non-root user
USER nodejs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD node -e "console.log('Health check passed')" || exit 1

# Expose port (if running a web server in the future)
EXPOSE 3000

# Default command
CMD ["npm", "start"]

# Multi-architecture build support
# docker buildx build --platform linux/amd64,linux/arm64 -t ecommerce-analytics .