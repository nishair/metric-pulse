import cron from 'node-cron';
import { ETLPipeline } from './pipelines/etlPipeline.js';
import { config, validateConfig } from './utils/config.js';
import { logger } from './utils/logger.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Ensure logs directory exists
const logsDir = path.join(__dirname, '../logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

async function runPipeline() {
  const pipeline = new ETLPipeline();

  try {
    logger.info('Starting scheduled ETL pipeline run');
    const results = await pipeline.run();
    logger.info('ETL pipeline completed successfully', results);
  } catch (error) {
    logger.error('ETL pipeline failed', error);
  } finally {
    await pipeline.close();
  }
}

async function main() {
  try {
    // Validate configuration
    validateConfig();
    logger.info('Configuration validated successfully');

    // Run pipeline immediately if specified
    if (process.argv.includes('--run-now')) {
      logger.info('Running pipeline immediately (--run-now flag detected)');
      await runPipeline();
    }

    // Set up scheduled runs
    if (process.argv.includes('--schedule')) {
      const schedule = config.pipeline.scheduleCron;
      logger.info(`Setting up scheduled pipeline runs with cron: ${schedule}`);

      cron.schedule(schedule, async () => {
        await runPipeline();
      });

      logger.info('E-commerce Analytics Pipeline scheduler started');
      logger.info(`Next run will be according to schedule: ${schedule}`);

      // Keep the process running
      process.on('SIGINT', () => {
        logger.info('Received SIGINT, shutting down gracefully');
        process.exit(0);
      });

      process.on('SIGTERM', () => {
        logger.info('Received SIGTERM, shutting down gracefully');
        process.exit(0);
      });
    } else if (!process.argv.includes('--run-now')) {
      // If neither flag is provided, show usage
      console.log(`
E-commerce Analytics Pipeline

Usage:
  npm start               - Show this help message
  npm start -- --run-now  - Run the pipeline immediately
  npm start -- --schedule - Start the scheduler for daily runs
  npm run migrate         - Run database migrations

Configuration:
  Edit the .env file to configure your data sources and database connection.
  See .env.example for all available options.

Current Configuration:
  - Shopify: ${config.shopify.enabled ? 'Enabled' : 'Disabled'}
  - WooCommerce: ${config.woocommerce.enabled ? 'Enabled' : 'Disabled'}
  - Schedule: ${config.pipeline.scheduleCron}
      `);
    }
  } catch (error) {
    logger.error('Failed to start pipeline', error);
    console.error('Failed to start pipeline:', error.message);
    process.exit(1);
  }
}

main();