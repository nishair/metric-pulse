import pg from 'pg';
import { logger } from '../utils/logger.js';

const { Pool } = pg;

export class PostgresLoader {
  constructor(config) {
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  async testConnection() {
    try {
      const client = await this.pool.connect();
      await client.query('SELECT NOW()');
      client.release();
      logger.info('PostgreSQL connection successful');
      return true;
    } catch (error) {
      logger.error('PostgreSQL connection failed', error);
      return false;
    }
  }

  async beginTransaction() {
    const client = await this.pool.connect();
    await client.query('BEGIN');
    return client;
  }

  async commitTransaction(client) {
    await client.query('COMMIT');
    client.release();
  }

  async rollbackTransaction(client) {
    await client.query('ROLLBACK');
    client.release();
  }

  // Customer operations
  async upsertCustomer(customer) {
    const query = `
      INSERT INTO customers (
        source_id, source_type, email, first_name, last_name,
        phone, city, state, country, postal_code,
        total_spent, orders_count, tags, first_purchase_date,
        last_purchase_date, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
      ON CONFLICT (source_id, source_type)
      DO UPDATE SET
        email = EXCLUDED.email,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        phone = EXCLUDED.phone,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        postal_code = EXCLUDED.postal_code,
        total_spent = EXCLUDED.total_spent,
        orders_count = EXCLUDED.orders_count,
        tags = EXCLUDED.tags,
        last_purchase_date = EXCLUDED.last_purchase_date,
        updated_at = CURRENT_TIMESTAMP
      RETURNING id`;

    const values = [
      customer.source_id,
      customer.source_type,
      customer.email,
      customer.first_name,
      customer.last_name,
      customer.phone,
      customer.city,
      customer.state,
      customer.country,
      customer.postal_code,
      customer.total_spent,
      customer.orders_count,
      customer.tags,
      customer.first_purchase_date,
      customer.last_purchase_date,
      customer.created_at,
      customer.updated_at,
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error upserting customer', { error, customer });
      throw error;
    }
  }

  async upsertCustomerBatch(customers) {
    const inserted = [];
    const failed = [];

    for (const customer of customers) {
      try {
        const id = await this.upsertCustomer(customer);
        inserted.push({ ...customer, id });
      } catch (error) {
        failed.push({ customer, error: error.message });
      }
    }

    return { inserted, failed };
  }

  // Product operations
  async upsertProduct(product) {
    const query = `
      INSERT INTO products (
        source_id, source_type, title, vendor, product_type,
        sku, price, compare_at_price, inventory_quantity,
        tags, status, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      ON CONFLICT (source_id, source_type)
      DO UPDATE SET
        title = EXCLUDED.title,
        vendor = EXCLUDED.vendor,
        product_type = EXCLUDED.product_type,
        sku = EXCLUDED.sku,
        price = EXCLUDED.price,
        compare_at_price = EXCLUDED.compare_at_price,
        inventory_quantity = EXCLUDED.inventory_quantity,
        tags = EXCLUDED.tags,
        status = EXCLUDED.status,
        updated_at = CURRENT_TIMESTAMP
      RETURNING id`;

    const values = [
      product.source_id,
      product.source_type,
      product.title,
      product.vendor,
      product.product_type,
      product.sku,
      product.price,
      product.compare_at_price,
      product.inventory_quantity,
      product.tags,
      product.status,
      product.created_at,
      product.updated_at,
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error upserting product', { error, product });
      throw error;
    }
  }

  async upsertProductBatch(products) {
    const inserted = [];
    const failed = [];

    for (const product of products) {
      try {
        const id = await this.upsertProduct(product);
        inserted.push({ ...product, id });
      } catch (error) {
        failed.push({ product, error: error.message });
      }
    }

    return { inserted, failed };
  }

  // Order operations
  async upsertOrder(order, customerId = null) {
    const query = `
      INSERT INTO orders (
        source_id, source_type, order_number, customer_id, email,
        financial_status, fulfillment_status, currency,
        subtotal_price, total_tax, total_discounts, total_shipping,
        total_price, processed_at, cancelled_at, tags,
        source_name, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
      ON CONFLICT (source_id, source_type)
      DO UPDATE SET
        order_number = EXCLUDED.order_number,
        customer_id = EXCLUDED.customer_id,
        email = EXCLUDED.email,
        financial_status = EXCLUDED.financial_status,
        fulfillment_status = EXCLUDED.fulfillment_status,
        currency = EXCLUDED.currency,
        subtotal_price = EXCLUDED.subtotal_price,
        total_tax = EXCLUDED.total_tax,
        total_discounts = EXCLUDED.total_discounts,
        total_shipping = EXCLUDED.total_shipping,
        total_price = EXCLUDED.total_price,
        processed_at = EXCLUDED.processed_at,
        cancelled_at = EXCLUDED.cancelled_at,
        tags = EXCLUDED.tags,
        source_name = EXCLUDED.source_name,
        updated_at = CURRENT_TIMESTAMP
      RETURNING id`;

    const values = [
      order.source_id,
      order.source_type,
      order.order_number,
      customerId || order.customer_id,
      order.email,
      order.financial_status,
      order.fulfillment_status,
      order.currency,
      order.subtotal_price,
      order.total_tax,
      order.total_discounts,
      order.total_shipping,
      order.total_price,
      order.processed_at,
      order.cancelled_at,
      order.tags,
      order.source_name,
      order.created_at,
      order.updated_at,
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error upserting order', { error, order });
      throw error;
    }
  }

  // Order items operations
  async insertOrderItems(orderItems, orderId) {
    if (!orderItems || orderItems.length === 0) return [];

    const query = `
      INSERT INTO order_items (
        order_id, product_id, source_product_id, source_variant_id,
        title, variant_title, sku, quantity, price, total_discount,
        fulfillment_status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      RETURNING id`;

    const insertedItems = [];

    for (const item of orderItems) {
      // Try to find the product
      const productId = await this.findProductId(item.source_product_id, item.source_type);

      const values = [
        orderId,
        productId,
        item.source_product_id,
        item.source_variant_id,
        item.title,
        item.variant_title,
        item.sku,
        item.quantity,
        item.price,
        item.total_discount,
        item.fulfillment_status,
      ];

      try {
        const result = await this.pool.query(query, values);
        insertedItems.push({ ...item, id: result.rows[0].id });
      } catch (error) {
        logger.error('Error inserting order item', { error, item });
      }
    }

    return insertedItems;
  }

  async findProductId(sourceProductId, sourceType) {
    if (!sourceProductId) return null;

    const query = `
      SELECT id FROM products
      WHERE source_id = $1 AND source_type = $2
      LIMIT 1`;

    try {
      const result = await this.pool.query(query, [sourceProductId, sourceType]);
      return result.rows[0]?.id || null;
    } catch (error) {
      logger.error('Error finding product', { error, sourceProductId });
      return null;
    }
  }

  async findCustomerId(email, sourceType) {
    if (!email) return null;

    const query = `
      SELECT id FROM customers
      WHERE email = $1 AND source_type = $2
      LIMIT 1`;

    try {
      const result = await this.pool.query(query, [email.toLowerCase(), sourceType]);
      return result.rows[0]?.id || null;
    } catch (error) {
      logger.error('Error finding customer', { error, email });
      return null;
    }
  }

  // Customer metrics operations
  async upsertCustomerMetrics(metrics) {
    const query = `
      INSERT INTO customer_metrics (
        customer_id, calculation_date, total_revenue, total_orders,
        average_order_value, purchase_frequency, customer_lifespan_days,
        customer_lifetime_value, churn_probability, days_since_last_purchase,
        rfm_recency_score, rfm_frequency_score, rfm_monetary_score,
        customer_segment
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (customer_id, calculation_date)
      DO UPDATE SET
        total_revenue = EXCLUDED.total_revenue,
        total_orders = EXCLUDED.total_orders,
        average_order_value = EXCLUDED.average_order_value,
        purchase_frequency = EXCLUDED.purchase_frequency,
        customer_lifespan_days = EXCLUDED.customer_lifespan_days,
        customer_lifetime_value = EXCLUDED.customer_lifetime_value,
        churn_probability = EXCLUDED.churn_probability,
        days_since_last_purchase = EXCLUDED.days_since_last_purchase,
        rfm_recency_score = EXCLUDED.rfm_recency_score,
        rfm_frequency_score = EXCLUDED.rfm_frequency_score,
        rfm_monetary_score = EXCLUDED.rfm_monetary_score,
        customer_segment = EXCLUDED.customer_segment
      RETURNING id`;

    const values = [
      metrics.customer_id,
      metrics.calculation_date,
      metrics.total_revenue,
      metrics.total_orders,
      metrics.average_order_value,
      metrics.purchase_frequency,
      metrics.customer_lifespan_days,
      metrics.customer_lifetime_value,
      metrics.churn_probability,
      metrics.days_since_last_purchase,
      metrics.rfm_recency_score,
      metrics.rfm_frequency_score,
      metrics.rfm_monetary_score,
      metrics.customer_segment,
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error upserting customer metrics', { error, metrics });
      throw error;
    }
  }

  // Daily metrics operations
  async upsertDailyMetrics(metrics) {
    const query = `
      INSERT INTO daily_metrics (
        metric_date, source_type, total_revenue, total_orders,
        total_customers, new_customers, returning_customers,
        average_order_value, total_products_sold, top_selling_products,
        revenue_by_source, conversion_rate, cart_abandonment_rate
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      ON CONFLICT (metric_date, source_type)
      DO UPDATE SET
        total_revenue = EXCLUDED.total_revenue,
        total_orders = EXCLUDED.total_orders,
        total_customers = EXCLUDED.total_customers,
        new_customers = EXCLUDED.new_customers,
        returning_customers = EXCLUDED.returning_customers,
        average_order_value = EXCLUDED.average_order_value,
        total_products_sold = EXCLUDED.total_products_sold,
        top_selling_products = EXCLUDED.top_selling_products,
        revenue_by_source = EXCLUDED.revenue_by_source,
        conversion_rate = EXCLUDED.conversion_rate,
        cart_abandonment_rate = EXCLUDED.cart_abandonment_rate
      RETURNING id`;

    const values = [
      metrics.metric_date,
      metrics.source_type,
      metrics.total_revenue,
      metrics.total_orders,
      metrics.total_customers,
      metrics.new_customers,
      metrics.returning_customers,
      metrics.average_order_value,
      metrics.total_products_sold,
      JSON.stringify(metrics.top_selling_products),
      JSON.stringify(metrics.revenue_by_source),
      metrics.conversion_rate,
      metrics.cart_abandonment_rate,
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error upserting daily metrics', { error, metrics });
      throw error;
    }
  }

  // ETL logging
  async logETLRun(logEntry) {
    const query = `
      INSERT INTO etl_logs (
        pipeline_name, source_type, status, records_extracted,
        records_transformed, records_loaded, error_message,
        started_at, completed_at, duration_seconds, metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      RETURNING id`;

    const values = [
      logEntry.pipeline_name,
      logEntry.source_type,
      logEntry.status,
      logEntry.records_extracted,
      logEntry.records_transformed,
      logEntry.records_loaded,
      logEntry.error_message,
      logEntry.started_at,
      logEntry.completed_at,
      logEntry.duration_seconds,
      JSON.stringify(logEntry.metadata || {}),
    ];

    try {
      const result = await this.pool.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      logger.error('Error logging ETL run', { error, logEntry });
      throw error;
    }
  }

  async getLastETLRun(pipelineName, sourceType) {
    const query = `
      SELECT * FROM etl_logs
      WHERE pipeline_name = $1 AND source_type = $2
        AND status = 'success'
      ORDER BY completed_at DESC
      LIMIT 1`;

    try {
      const result = await this.pool.query(query, [pipelineName, sourceType]);
      return result.rows[0] || null;
    } catch (error) {
      logger.error('Error getting last ETL run', { error });
      return null;
    }
  }

  async close() {
    await this.pool.end();
  }
}