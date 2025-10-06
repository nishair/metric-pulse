import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'assert';
import { PostgresLoader } from '../../src/db/postgresLoader.js';
import { MockDatabase, createMockLogger } from '../utils/test-helpers.js';
import { transformedCustomer, transformedProduct, transformedOrder } from '../fixtures/transformed-data.js';

describe('PostgresLoader', () => {
  let loader;
  let mockDb;
  let originalPool;

  beforeEach(() => {
    mockDb = new MockDatabase();

    const config = {
      host: 'localhost',
      port: 5432,
      database: 'test_db',
      user: 'test_user',
      password: 'test_password',
    };

    loader = new PostgresLoader(config);

    // Replace the pool with our mock
    originalPool = loader.pool;
    loader.pool = {
      connect: () => mockDb.connect(),
      query: (sql, params) => mockDb.query(sql, params),
      end: () => Promise.resolve(),
    };
  });

  afterEach(() => {
    if (originalPool) {
      loader.pool = originalPool;
    }
  });

  describe('constructor', () => {
    it('should initialize with correct configuration', () => {
      const config = {
        host: 'localhost',
        port: 5432,
        database: 'test_db',
        user: 'test_user',
        password: 'test_password',
      };

      const newLoader = new PostgresLoader(config);
      assert.ok(newLoader.pool);
    });
  });

  describe('testConnection', () => {
    it('should return true on successful connection', async () => {
      const result = await loader.testConnection();
      assert.equal(result, true);
    });

    it('should return false on connection failure', async () => {
      mockDb.setShouldFail(true, 'Connection failed');

      const result = await loader.testConnection();
      assert.equal(result, false);
    });
  });

  describe('upsertCustomer', () => {
    it('should insert customer successfully', async () => {
      mockDb.setData('customers', [{ id: 123 }]);

      const customerId = await loader.upsertCustomer(transformedCustomer);

      assert.ok(typeof customerId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO customers'));
      assert.ok(queries[0].sql.includes('ON CONFLICT'));
    });

    it('should handle customer upsert parameters correctly', async () => {
      await loader.upsertCustomer(transformedCustomer);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      assert.equal(params[0], transformedCustomer.source_id);
      assert.equal(params[1], transformedCustomer.source_type);
      assert.equal(params[2], transformedCustomer.email);
      assert.equal(params[3], transformedCustomer.first_name);
      assert.equal(params[4], transformedCustomer.last_name);
    });

    it('should throw error on database failure', async () => {
      mockDb.setShouldFail(true, 'Database error');

      await assert.rejects(
        () => loader.upsertCustomer(transformedCustomer),
        /Database error/
      );
    });
  });

  describe('upsertCustomerBatch', () => {
    it('should process customer batch successfully', async () => {
      const customers = [transformedCustomer];

      const result = await loader.upsertCustomerBatch(customers);

      assert.equal(result.inserted.length, 1);
      assert.equal(result.failed.length, 0);
      assert.ok(result.inserted[0].id);
    });

    it('should handle partial failures in batch', async () => {
      const customers = [
        transformedCustomer,
        { ...transformedCustomer, source_id: 'invalid' },
      ];

      // Set up mock to fail on second insert
      let callCount = 0;
      const originalQuery = mockDb.query.bind(mockDb);
      mockDb.query = async function(sql, params) {
        callCount++;
        if (callCount === 2) {
          throw new Error('Duplicate key error');
        }
        return originalQuery(sql, params);
      };

      const result = await loader.upsertCustomerBatch(customers);

      assert.equal(result.inserted.length, 1);
      assert.equal(result.failed.length, 1);
      assert.equal(result.failed[0].error, 'Duplicate key error');
    });
  });

  describe('upsertProduct', () => {
    it('should insert product successfully', async () => {
      const productId = await loader.upsertProduct(transformedProduct);

      assert.ok(typeof productId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO products'));
    });

    it('should handle product parameters correctly', async () => {
      await loader.upsertProduct(transformedProduct);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      assert.equal(params[0], transformedProduct.source_id);
      assert.equal(params[1], transformedProduct.source_type);
      assert.equal(params[2], transformedProduct.title);
      assert.equal(params[3], transformedProduct.vendor);
    });
  });

  describe('upsertOrder', () => {
    it('should insert order successfully', async () => {
      const orderId = await loader.upsertOrder(transformedOrder);

      assert.ok(typeof orderId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO orders'));
    });

    it('should handle order with customer ID', async () => {
      const customerId = 123;
      await loader.upsertOrder(transformedOrder, customerId);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      assert.equal(params[3], customerId); // customer_id parameter
    });

    it('should handle order parameters correctly', async () => {
      await loader.upsertOrder(transformedOrder);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      assert.equal(params[0], transformedOrder.source_id);
      assert.equal(params[1], transformedOrder.source_type);
      assert.equal(params[2], transformedOrder.order_number);
      assert.equal(params[4], transformedOrder.email);
    });
  });

  describe('insertOrderItems', () => {
    const mockOrderItems = [
      {
        source_product_id: '111222333',
        source_variant_id: '444555666',
        title: 'Test Product',
        quantity: 2,
        price: 29.99,
        sku: 'TEST-001',
      },
    ];

    it('should insert order items successfully', async () => {
      const orderId = 123;
      const result = await loader.insertOrderItems(mockOrderItems, orderId);

      assert.equal(result.length, 1);
      assert.ok(result[0].id);

      const queries = mockDb.getQueries();
      // Should have 2 queries: one to find product, one to insert item
      assert.ok(queries.length >= 1);
    });

    it('should handle empty order items', async () => {
      const result = await loader.insertOrderItems([], 123);
      assert.equal(result.length, 0);
    });

    it('should handle null order items', async () => {
      const result = await loader.insertOrderItems(null, 123);
      assert.equal(result.length, 0);
    });
  });

  describe('findProductId', () => {
    it('should find existing product', async () => {
      mockDb.setData('products', [{ id: 456 }]);

      const productId = await loader.findProductId('111222333', 'shopify');

      assert.equal(productId, 456);

      const queries = mockDb.getQueries();
      assert.ok(queries[0].sql.includes('SELECT id FROM products'));
    });

    it('should return null for non-existent product', async () => {
      mockDb.setData('products', []);

      const productId = await loader.findProductId('999', 'shopify');

      assert.equal(productId, null);
    });

    it('should return null for null sourceProductId', async () => {
      const productId = await loader.findProductId(null, 'shopify');
      assert.equal(productId, null);
    });
  });

  describe('findCustomerId', () => {
    it('should find existing customer by email', async () => {
      mockDb.setData('customers', [{ id: 789 }]);

      const customerId = await loader.findCustomerId('test@example.com', 'shopify');

      assert.equal(customerId, 789);

      const queries = mockDb.getQueries();
      assert.ok(queries[0].sql.includes('SELECT id FROM customers'));
      assert.equal(queries[0].params[0], 'test@example.com');
    });

    it('should return null for non-existent customer', async () => {
      mockDb.setData('customers', []);

      const customerId = await loader.findCustomerId('notfound@example.com', 'shopify');

      assert.equal(customerId, null);
    });

    it('should return null for null email', async () => {
      const customerId = await loader.findCustomerId(null, 'shopify');
      assert.equal(customerId, null);
    });
  });

  describe('upsertCustomerMetrics', () => {
    const mockMetrics = {
      customer_id: 1,
      calculation_date: new Date('2024-01-15'),
      total_revenue: 1000.00,
      total_orders: 5,
      average_order_value: 200.00,
      purchase_frequency: 2.5,
      customer_lifespan_days: 180,
      customer_lifetime_value: 2500.00,
      churn_probability: 0.15,
      days_since_last_purchase: 30,
      rfm_recency_score: 4,
      rfm_frequency_score: 4,
      rfm_monetary_score: 5,
      customer_segment: 'Champions',
    };

    it('should insert customer metrics successfully', async () => {
      const metricsId = await loader.upsertCustomerMetrics(mockMetrics);

      assert.ok(typeof metricsId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO customer_metrics'));
      assert.ok(queries[0].sql.includes('ON CONFLICT'));
    });

    it('should handle metrics parameters correctly', async () => {
      await loader.upsertCustomerMetrics(mockMetrics);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      assert.equal(params[0], mockMetrics.customer_id);
      assert.equal(params[2], mockMetrics.total_revenue);
      assert.equal(params[7], mockMetrics.customer_lifetime_value);
      assert.equal(params[13], mockMetrics.customer_segment);
    });
  });

  describe('upsertDailyMetrics', () => {
    const mockDailyMetrics = {
      metric_date: new Date('2024-01-15'),
      source_type: 'shopify',
      total_revenue: 5000.00,
      total_orders: 25,
      total_customers: 15,
      new_customers: 5,
      returning_customers: 10,
      average_order_value: 200.00,
      total_products_sold: 50,
      top_selling_products: [
        { product_id: '1', title: 'Product A', quantity: 10, revenue: 500 },
      ],
      revenue_by_source: { web: 3000, mobile: 2000 },
      conversion_rate: 0.05,
      cart_abandonment_rate: 0.70,
    };

    it('should insert daily metrics successfully', async () => {
      const metricsId = await loader.upsertDailyMetrics(mockDailyMetrics);

      assert.ok(typeof metricsId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO daily_metrics'));
      assert.ok(queries[0].sql.includes('ON CONFLICT'));
    });

    it('should handle JSON fields correctly', async () => {
      await loader.upsertDailyMetrics(mockDailyMetrics);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      // Check that JSON fields are stringified
      const topProductsParam = params[9];
      const revenueBySourceParam = params[10];

      assert.equal(typeof topProductsParam, 'string');
      assert.equal(typeof revenueBySourceParam, 'string');

      // Verify JSON is parseable
      assert.doesNotThrow(() => JSON.parse(topProductsParam));
      assert.doesNotThrow(() => JSON.parse(revenueBySourceParam));
    });
  });

  describe('logETLRun', () => {
    const mockLogEntry = {
      pipeline_name: 'test_pipeline',
      source_type: 'shopify',
      status: 'success',
      records_extracted: 100,
      records_transformed: 95,
      records_loaded: 95,
      error_message: null,
      started_at: new Date('2024-01-15T10:00:00Z'),
      completed_at: new Date('2024-01-15T10:30:00Z'),
      duration_seconds: 1800,
      metadata: { test: 'data' },
    };

    it('should log ETL run successfully', async () => {
      const logId = await loader.logETLRun(mockLogEntry);

      assert.ok(typeof logId === 'number');

      const queries = mockDb.getQueries();
      assert.equal(queries.length, 1);
      assert.ok(queries[0].sql.includes('INSERT INTO etl_logs'));
    });

    it('should handle metadata JSON correctly', async () => {
      await loader.logETLRun(mockLogEntry);

      const queries = mockDb.getQueries();
      const params = queries[0].params;

      const metadataParam = params[10];
      assert.equal(typeof metadataParam, 'string');
      assert.doesNotThrow(() => JSON.parse(metadataParam));
    });
  });

  describe('getLastETLRun', () => {
    it('should retrieve last successful ETL run', async () => {
      const mockETLRun = {
        id: 1,
        pipeline_name: 'test_pipeline',
        source_type: 'shopify',
        status: 'success',
        completed_at: new Date('2024-01-15T10:00:00Z'),
      };

      mockDb.setData('etl_logs', [mockETLRun]);

      const result = await loader.getLastETLRun('test_pipeline', 'shopify');

      assert.deepEqual(result, mockETLRun);

      const queries = mockDb.getQueries();
      assert.ok(queries[0].sql.includes('SELECT * FROM etl_logs'));
      assert.ok(queries[0].sql.includes('status = \'success\''));
      assert.ok(queries[0].sql.includes('ORDER BY completed_at DESC'));
    });

    it('should return null when no successful runs found', async () => {
      mockDb.setData('etl_logs', []);

      const result = await loader.getLastETLRun('test_pipeline', 'shopify');

      assert.equal(result, null);
    });
  });

  describe('transaction management', () => {
    it('should begin transaction successfully', async () => {
      const client = await loader.beginTransaction();

      assert.ok(client);
      assert.equal(typeof client.query, 'function');

      const queries = mockDb.getQueries();
      assert.ok(queries.some(q => q.sql === 'BEGIN'));
    });

    it('should commit transaction successfully', async () => {
      const client = await loader.beginTransaction();
      await loader.commitTransaction(client);

      const queries = mockDb.getQueries();
      assert.ok(queries.some(q => q.sql === 'COMMIT'));
    });

    it('should rollback transaction successfully', async () => {
      const client = await loader.beginTransaction();
      await loader.rollbackTransaction(client);

      const queries = mockDb.getQueries();
      assert.ok(queries.some(q => q.sql === 'ROLLBACK'));
    });
  });

  describe('error handling', () => {
    it('should handle database connection errors gracefully', async () => {
      mockDb.setShouldFail(true, 'Connection timeout');

      await assert.rejects(
        () => loader.upsertCustomer(transformedCustomer),
        /Connection timeout/
      );
    });

    it('should handle malformed data gracefully', async () => {
      const malformedCustomer = {
        source_id: null, // This might cause issues
        source_type: 'shopify',
      };

      // The loader should still attempt the operation
      await assert.rejects(
        () => loader.upsertCustomer(malformedCustomer),
        Error
      );
    });
  });

  describe('close', () => {
    it('should close connection pool', async () => {
      let poolClosed = false;
      loader.pool.end = () => {
        poolClosed = true;
        return Promise.resolve();
      };

      await loader.close();
      assert.equal(poolClosed, true);
    });
  });
});