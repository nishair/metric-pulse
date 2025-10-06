import { ShopifyConnector } from '../connectors/shopify.js';
import { WooCommerceConnector } from '../connectors/woocommerce.js';
import { CommercetoolsConnector } from '../connectors/commercetools.js';
import { DataTransformer } from '../transformers/dataTransformer.js';
import { CLVCalculator } from '../analytics/clvCalculator.js';
import { PostgresLoader } from '../db/postgresLoader.js';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';

export class ETLPipeline {
  constructor() {
    this.transformer = new DataTransformer();
    this.clvCalculator = new CLVCalculator();
    this.loader = new PostgresLoader(config.database);
    this.connectors = {};

    // Initialize connectors based on configuration
    if (config.shopify.enabled) {
      this.connectors.shopify = new ShopifyConnector(config.shopify);
    }

    if (config.woocommerce.enabled) {
      this.connectors.woocommerce = new WooCommerceConnector(config.woocommerce);
    }

    if (config.commercetools.enabled) {
      this.connectors.commercetools = new CommercetoolsConnector(config.commercetools);
    }
  }

  async run() {
    const startTime = Date.now();
    const results = {
      shopify: null,
      woocommerce: null,
      commercetools: null,
    };

    try {
      logger.info('Starting ETL pipeline run');

      // Run Shopify pipeline
      if (config.shopify.enabled) {
        results.shopify = await this.runForSource('shopify');
      }

      // Run WooCommerce pipeline
      if (config.woocommerce.enabled) {
        results.woocommerce = await this.runForSource('woocommerce');
      }

      // Run Commercetools pipeline
      if (config.commercetools.enabled) {
        results.commercetools = await this.runForSource('commercetools');
      }

      const duration = (Date.now() - startTime) / 1000;
      logger.info(`ETL pipeline completed in ${duration} seconds`, results);

      return results;
    } catch (error) {
      logger.error('ETL pipeline failed', error);
      throw error;
    }
  }

  async runForSource(sourceType) {
    const etlLog = {
      pipeline_name: 'main_etl',
      source_type: sourceType,
      status: 'running',
      records_extracted: 0,
      records_transformed: 0,
      records_loaded: 0,
      started_at: new Date(),
      metadata: {},
    };

    try {
      logger.info(`Starting ETL for ${sourceType}`);

      const connector = this.connectors[sourceType];
      if (!connector) {
        throw new Error(`Connector for ${sourceType} not initialized`);
      }

      // Test connection
      const connectionOk = await connector.testConnection();
      if (!connectionOk) {
        throw new Error(`Failed to connect to ${sourceType}`);
      }

      // Get last successful run to determine incremental load
      const lastRun = await this.loader.getLastETLRun('main_etl', sourceType);
      const since = lastRun ? new Date(lastRun.completed_at) : null;

      // Extract data
      logger.info(`Extracting data from ${sourceType}`, { since });
      const extractedData = await this.extractData(connector, since);
      etlLog.records_extracted =
        extractedData.customers.length +
        extractedData.products.length +
        extractedData.orders.length;

      // Transform data
      logger.info(`Transforming ${sourceType} data`);
      const transformedData = await this.transformData(extractedData, sourceType);
      etlLog.records_transformed =
        transformedData.customers.length +
        transformedData.products.length +
        transformedData.orders.length;

      // Load data
      logger.info(`Loading ${sourceType} data to PostgreSQL`);
      const loadResults = await this.loadData(transformedData, sourceType);
      etlLog.records_loaded = loadResults.totalLoaded;

      // Calculate and store metrics
      logger.info(`Calculating metrics for ${sourceType}`);
      await this.calculateMetrics(transformedData, sourceType);

      // Mark as successful
      etlLog.status = 'success';
      etlLog.completed_at = new Date();
      etlLog.duration_seconds = Math.floor((etlLog.completed_at - etlLog.started_at) / 1000);
      etlLog.metadata = loadResults;

      await this.loader.logETLRun(etlLog);

      return {
        success: true,
        ...etlLog,
      };

    } catch (error) {
      logger.error(`ETL failed for ${sourceType}`, error);

      etlLog.status = 'failed';
      etlLog.error_message = error.message;
      etlLog.completed_at = new Date();
      etlLog.duration_seconds = Math.floor((etlLog.completed_at - etlLog.started_at) / 1000);

      await this.loader.logETLRun(etlLog);

      return {
        success: false,
        ...etlLog,
      };
    }
  }

  async extractData(connector, since) {
    const data = {
      customers: [],
      products: [],
      orders: [],
    };

    try {
      // Extract customers
      data.customers = await connector.getCustomers(since);
      logger.info(`Extracted ${data.customers.length} customers`);

      // Extract products
      data.products = await connector.getProducts(since);
      logger.info(`Extracted ${data.products.length} products`);

      // Extract orders
      data.orders = await connector.getOrders(since);
      logger.info(`Extracted ${data.orders.length} orders`);

    } catch (error) {
      logger.error('Data extraction failed', error);
      throw error;
    }

    return data;
  }

  async transformData(data, sourceType) {
    const transformed = {
      customers: [],
      products: [],
      orders: [],
      orderItems: [],
    };

    try {
      // Transform customers
      transformed.customers = this.transformer.transformCustomerBatch(
        data.customers,
        sourceType
      );

      // Transform products
      transformed.products = this.transformer.transformProductBatch(
        data.products,
        sourceType
      );

      // Transform orders and order items
      for (const order of data.orders) {
        const transformedOrder = this.transformer.transformOrder(order, sourceType);
        transformed.orders.push(transformedOrder);

        // Transform order items if they exist
        if (order.line_items) {
          for (const item of order.line_items) {
            const transformedItem = this.transformer.transformOrderItem(
              item,
              null, // Will be set during loading
              sourceType
            );
            transformedItem.source_order_id = transformedOrder.source_id;
            transformed.orderItems.push(transformedItem);
          }
        }
      }

      logger.info('Data transformation completed', {
        customers: transformed.customers.length,
        products: transformed.products.length,
        orders: transformed.orders.length,
        orderItems: transformed.orderItems.length,
      });

    } catch (error) {
      logger.error('Data transformation failed', error);
      throw error;
    }

    return transformed;
  }

  async loadData(data, sourceType) {
    const results = {
      customers: { inserted: [], failed: [] },
      products: { inserted: [], failed: [] },
      orders: { inserted: [], failed: [] },
      orderItems: { inserted: [], failed: [] },
      totalLoaded: 0,
    };

    try {
      // Load customers
      logger.info('Loading customers');
      results.customers = await this.loader.upsertCustomerBatch(data.customers);

      // Load products
      logger.info('Loading products');
      results.products = await this.loader.upsertProductBatch(data.products);

      // Load orders with customer linking
      logger.info('Loading orders');
      for (const order of data.orders) {
        try {
          // Find customer ID if email exists
          let customerId = null;
          if (order.email) {
            customerId = await this.loader.findCustomerId(order.email, sourceType);
          }

          const orderId = await this.loader.upsertOrder(order, customerId);

          // Load order items for this order
          const orderItems = data.orderItems.filter(
            item => item.source_order_id === order.source_id
          );

          if (orderItems.length > 0) {
            const insertedItems = await this.loader.insertOrderItems(orderItems, orderId);
            results.orderItems.inserted.push(...insertedItems);
          }

          results.orders.inserted.push({ ...order, id: orderId });
        } catch (error) {
          results.orders.failed.push({ order, error: error.message });
        }
      }

      // Update customer first/last purchase dates
      await this.updateCustomerPurchaseDates(sourceType);

      results.totalLoaded =
        results.customers.inserted.length +
        results.products.inserted.length +
        results.orders.inserted.length +
        results.orderItems.inserted.length;

      logger.info('Data loading completed', results);

    } catch (error) {
      logger.error('Data loading failed', error);
      throw error;
    }

    return results;
  }

  async updateCustomerPurchaseDates(sourceType) {
    const query = `
      UPDATE customers c
      SET
        first_purchase_date = COALESCE(
          (SELECT MIN(processed_at) FROM orders WHERE customer_id = c.id),
          first_purchase_date
        ),
        last_purchase_date = COALESCE(
          (SELECT MAX(processed_at) FROM orders WHERE customer_id = c.id),
          last_purchase_date
        )
      WHERE source_type = $1`;

    try {
      await this.loader.pool.query(query, [sourceType]);
      logger.info('Updated customer purchase dates');
    } catch (error) {
      logger.error('Failed to update customer purchase dates', error);
    }
  }

  async calculateMetrics(data, sourceType) {
    try {
      const calculationDate = new Date();

      // Get all customers with their orders for CLV calculation
      const customersQuery = `
        SELECT c.*,
          array_agg(
            json_build_object(
              'id', o.id,
              'customer_id', o.customer_id,
              'total_price', o.total_price,
              'processed_at', o.processed_at,
              'financial_status', o.financial_status
            ) ORDER BY o.processed_at
          ) FILTER (WHERE o.id IS NOT NULL) as orders
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        WHERE c.source_type = $1
        GROUP BY c.id`;

      const result = await this.loader.pool.query(customersQuery, [sourceType]);
      const customersWithOrders = result.rows;

      // Calculate CLV for each customer
      for (const customer of customersWithOrders) {
        if (customer.orders && customer.orders.length > 0) {
          const metrics = this.clvCalculator.calculateCustomerMetrics(
            customer,
            customer.orders,
            calculationDate
          );

          await this.loader.upsertCustomerMetrics(metrics);
        }
      }

      // Calculate daily metrics
      const ordersQuery = `
        SELECT o.*,
          array_agg(
            json_build_object(
              'source_product_id', oi.source_product_id,
              'title', oi.title,
              'quantity', oi.quantity,
              'price', oi.price
            )
          ) FILTER (WHERE oi.id IS NOT NULL) as line_items
        FROM orders o
        LEFT JOIN order_items oi ON oi.order_id = o.id
        WHERE o.source_type = $1
          AND DATE(o.processed_at) = DATE($2)
        GROUP BY o.id`;

      const ordersResult = await this.loader.pool.query(ordersQuery, [
        sourceType,
        calculationDate,
      ]);

      const dailyOrders = ordersResult.rows;
      const dailyMetrics = this.clvCalculator.calculateDailyMetrics(
        dailyOrders,
        data.products,
        calculationDate
      );

      dailyMetrics.source_type = sourceType;
      await this.loader.upsertDailyMetrics(dailyMetrics);

      logger.info(`Metrics calculation completed for ${sourceType}`);

    } catch (error) {
      logger.error('Metrics calculation failed', error);
      throw error;
    }
  }

  async close() {
    await this.loader.close();
  }
}