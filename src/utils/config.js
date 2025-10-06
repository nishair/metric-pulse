import dotenv from 'dotenv';

dotenv.config();

export const config = {
  // Shopify configuration
  shopify: {
    enabled: process.env.ENABLE_SHOPIFY === 'true',
    storeUrl: process.env.SHOPIFY_STORE_URL,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
  },

  // WooCommerce configuration
  woocommerce: {
    enabled: process.env.ENABLE_WOOCOMMERCE === 'true',
    url: process.env.WOOCOMMERCE_URL,
    consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
    consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  },

  // Commercetools configuration
  commercetools: {
    enabled: process.env.ENABLE_COMMERCETOOLS === 'true',
    projectKey: process.env.COMMERCETOOLS_PROJECT_KEY,
    clientId: process.env.COMMERCETOOLS_CLIENT_ID,
    clientSecret: process.env.COMMERCETOOLS_CLIENT_SECRET,
    region: process.env.COMMERCETOOLS_REGION || 'us-central1',
  },

  // Database configuration
  database: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'ecommerce_analytics',
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
  },

  // Pipeline configuration
  pipeline: {
    scheduleCron: process.env.SCHEDULE_CRON || '0 2 * * *', // Default: 2 AM daily
    batchSize: parseInt(process.env.BATCH_SIZE || '100'),
    retryAttempts: parseInt(process.env.RETRY_ATTEMPTS || '3'),
    retryDelay: parseInt(process.env.RETRY_DELAY || '5000'), // milliseconds
  },

  // Logging configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info',
  },
};

// Validate configuration
export function validateConfig() {
  const errors = [];

  if (config.shopify.enabled) {
    if (!config.shopify.storeUrl) {
      errors.push('SHOPIFY_STORE_URL is required when Shopify is enabled');
    }
    if (!config.shopify.accessToken) {
      errors.push('SHOPIFY_ACCESS_TOKEN is required when Shopify is enabled');
    }
  }

  if (config.woocommerce.enabled) {
    if (!config.woocommerce.url) {
      errors.push('WOOCOMMERCE_URL is required when WooCommerce is enabled');
    }
    if (!config.woocommerce.consumerKey) {
      errors.push('WOOCOMMERCE_CONSUMER_KEY is required when WooCommerce is enabled');
    }
    if (!config.woocommerce.consumerSecret) {
      errors.push('WOOCOMMERCE_CONSUMER_SECRET is required when WooCommerce is enabled');
    }
  }

  if (config.commercetools.enabled) {
    if (!config.commercetools.projectKey) {
      errors.push('COMMERCETOOLS_PROJECT_KEY is required when Commercetools is enabled');
    }
    if (!config.commercetools.clientId) {
      errors.push('COMMERCETOOLS_CLIENT_ID is required when Commercetools is enabled');
    }
    if (!config.commercetools.clientSecret) {
      errors.push('COMMERCETOOLS_CLIENT_SECRET is required when Commercetools is enabled');
    }
  }

  if (!config.shopify.enabled && !config.woocommerce.enabled && !config.commercetools.enabled) {
    errors.push('At least one data source (Shopify, WooCommerce, or Commercetools) must be enabled');
  }

  if (!config.database.user) {
    errors.push('DB_USER is required');
  }

  if (!config.database.password) {
    errors.push('DB_PASSWORD is required');
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }

  return true;
}