import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const { Pool } = pg;

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

async function runMigration() {
  try {
    console.log('Running database migration...');

    const schemaPath = path.join(__dirname, 'schema.sql');
    const schema = fs.readFileSync(schemaPath, 'utf8');

    await pool.query(schema);

    console.log('Database migration completed successfully');

    // Insert default data sources
    await pool.query(`
      INSERT INTO data_sources (name, type, is_active)
      VALUES
        ('Primary Shopify Store', 'shopify', $1),
        ('Primary WooCommerce Store', 'woocommerce', $2),
        ('Primary Commercetools Store', 'commercetools', $3)
      ON CONFLICT (name) DO NOTHING
    `, [
      process.env.ENABLE_SHOPIFY === 'true',
      process.env.ENABLE_WOOCOMMERCE === 'true',
      process.env.ENABLE_COMMERCETOOLS === 'true'
    ]);

    console.log('Default data sources added');

  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

runMigration();