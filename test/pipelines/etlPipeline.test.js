import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'assert';
import { ETLPipeline } from '../../src/pipelines/etlPipeline.js';
import { MockConnector, MockDatabase, createMockLogger } from '../utils/test-helpers.js';
import { shopifyCustomers, shopifyProducts, shopifyOrders } from '../fixtures/shopify-data.js';

// Mock the config module
let mockConfig = {
  shopify: { enabled: true },
  woocommerce: { enabled: false },
  commercetools: { enabled: false },
  database: {
    host: 'localhost',
    port: 5432,
    database: 'test_db',
    user: 'test_user',
    password: 'test_password',
  },
};

describe('ETLPipeline', () => {
  let pipeline;
  let mockShopifyConnector;
  let mockDatabase;

  beforeEach(() => {
    // Create mock connectors with test data
    mockShopifyConnector = new MockConnector({
      customers: shopifyCustomers,
      products: shopifyProducts,
      orders: shopifyOrders,
    });

    mockDatabase = new MockDatabase();

    // Create pipeline instance
    pipeline = new ETLPipeline();

    // Replace connectors with mocks
    pipeline.connectors = {
      shopify: mockShopifyConnector,
    };

    // Replace loader with mock
    pipeline.loader = {
      pool: mockDatabase,
      testConnection: () => Promise.resolve(true),
      getLastETLRun: () => Promise.resolve(null),
      upsertCustomerBatch: (customers) => Promise.resolve({
        inserted: customers.map((c, i) => ({ ...c, id: i + 1 })),
        failed: [],
      }),
      upsertProductBatch: (products) => Promise.resolve({
        inserted: products.map((p, i) => ({ ...p, id: i + 1 })),
        failed: [],
      }),
      upsertOrder: (order) => Promise.resolve(Math.floor(Math.random() * 1000)),
      insertOrderItems: (items) => Promise.resolve(items.map((item, i) => ({ ...item, id: i + 1 }))),
      findCustomerId: () => Promise.resolve(1),
      upsertCustomerMetrics: () => Promise.resolve(1),
      upsertDailyMetrics: () => Promise.resolve(1),
      logETLRun: () => Promise.resolve(1),
      close: () => Promise.resolve(),
    };
  });

  afterEach(async () => {
    if (pipeline) {
      await pipeline.close();
    }
  });

  describe('constructor', () => {
    it('should initialize with connectors based on config', () => {
      const newPipeline = new ETLPipeline();

      // In real implementation, this would check actual config
      assert.ok(newPipeline.transformer);
      assert.ok(newPipeline.clvCalculator);
      assert.ok(newPipeline.loader);
    });
  });

  describe('run', () => {
    it('should execute pipeline for all enabled sources', async () => {
      const results = await pipeline.run();

      assert.ok(results);
      assert.ok(results.shopify);
      assert.equal(results.shopify.success, true);
      assert.ok(results.shopify.records_extracted > 0);
      assert.ok(results.shopify.records_transformed > 0);
      assert.ok(results.shopify.records_loaded > 0);
    });

    it('should handle pipeline failures gracefully', async () => {
      // Make connector fail
      mockShopifyConnector.setConnectionTestResult(false);

      const results = await pipeline.run();

      assert.ok(results);
      assert.ok(results.shopify);
      assert.equal(results.shopify.success, false);
      assert.ok(results.shopify.error_message);
    });

    it('should measure execution time', async () => {
      const results = await pipeline.run();

      assert.ok(results.shopify.started_at);
      assert.ok(results.shopify.completed_at);
      assert.ok(results.shopify.duration_seconds >= 0);
    });
  });

  describe('runForSource', () => {
    it('should execute complete ETL process for Shopify', async () => {
      const result = await pipeline.runForSource('shopify');

      assert.equal(result.success, true);
      assert.equal(result.source_type, 'shopify');
      assert.equal(result.records_extracted, 7); // 2 customers + 2 products + 2 orders + 1 analytics call
      assert.ok(result.records_transformed > 0);
      assert.ok(result.records_loaded > 0);
    });

    it('should handle missing connector', async () => {
      const result = await pipeline.runForSource('nonexistent');

      assert.equal(result.success, false);
      assert.ok(result.error_message.includes('not initialized'));
    });

    it('should handle connection failures', async () => {
      mockShopifyConnector.setConnectionTestResult(false);

      const result = await pipeline.runForSource('shopify');

      assert.equal(result.success, false);
      assert.ok(result.error_message.includes('Failed to connect'));
    });

    it('should support incremental loading', async () => {
      const mockLastRun = {
        completed_at: new Date('2024-01-01T00:00:00Z'),
      };

      pipeline.loader.getLastETLRun = () => Promise.resolve(mockLastRun);

      // Track calls to connector methods
      let customersCall = null;
      const originalGetCustomers = mockShopifyConnector.getCustomers.bind(mockShopifyConnector);
      mockShopifyConnector.getCustomers = async function(since) {
        customersCall = { since };
        return originalGetCustomers(since);
      };

      await pipeline.runForSource('shopify');

      assert.ok(customersCall);
      assert.ok(customersCall.since instanceof Date);
    });
  });

  describe('extractData', () => {
    it('should extract all data types from connector', async () => {
      const data = await pipeline.extractData(mockShopifyConnector);

      assert.equal(data.customers.length, 2);
      assert.equal(data.products.length, 2);
      assert.equal(data.orders.length, 2);

      assert.equal(data.customers[0].email, 'john.doe@example.com');
      assert.equal(data.products[0].title, 'Premium T-Shirt');
      assert.equal(data.orders[0].order_number, '1001');
    });

    it('should handle extraction errors', async () => {
      // Make getCustomers fail
      mockShopifyConnector.getCustomers = async () => {
        throw new Error('API rate limit exceeded');
      };

      await assert.rejects(
        () => pipeline.extractData(mockShopifyConnector),
        /API rate limit exceeded/
      );
    });

    it('should pass date filter to connector methods', async () => {
      const since = new Date('2024-01-01');
      let datesPassed = [];

      // Track date parameters
      const originalGetCustomers = mockShopifyConnector.getCustomers.bind(mockShopifyConnector);
      const originalGetProducts = mockShopifyConnector.getProducts.bind(mockShopifyConnector);
      const originalGetOrders = mockShopifyConnector.getOrders.bind(mockShopifyConnector);

      mockShopifyConnector.getCustomers = async function(sinceDate) {
        datesPassed.push({ method: 'customers', since: sinceDate });
        return originalGetCustomers(sinceDate);
      };

      mockShopifyConnector.getProducts = async function(sinceDate) {
        datesPassed.push({ method: 'products', since: sinceDate });
        return originalGetProducts(sinceDate);
      };

      mockShopifyConnector.getOrders = async function(sinceDate) {
        datesPassed.push({ method: 'orders', since: sinceDate });
        return originalGetOrders(sinceDate);
      };

      await pipeline.extractData(mockShopifyConnector, since);

      assert.equal(datesPassed.length, 3);
      datesPassed.forEach(call => {
        assert.deepEqual(call.since, since);
      });
    });
  });

  describe('transformData', () => {
    it('should transform all data types correctly', async () => {
      const rawData = {
        customers: shopifyCustomers,
        products: shopifyProducts,
        orders: shopifyOrders,
      };

      const transformedData = await pipeline.transformData(rawData, 'shopify');

      assert.equal(transformedData.customers.length, 2);
      assert.equal(transformedData.products.length, 2);
      assert.equal(transformedData.orders.length, 2);
      assert.ok(transformedData.orderItems.length > 0);

      // Check transformation quality
      const customer = transformedData.customers[0];
      assert.equal(customer.source_type, 'shopify');
      assert.equal(customer.email, 'john.doe@example.com');

      const product = transformedData.products[0];
      assert.equal(product.source_type, 'shopify');
      assert.equal(product.title, 'Premium T-Shirt');

      const order = transformedData.orders[0];
      assert.equal(order.source_type, 'shopify');
      assert.equal(order.order_number, '1001');
    });

    it('should link order items to orders', async () => {
      const rawData = {
        customers: [],
        products: [],
        orders: shopifyOrders,
      };

      const transformedData = await pipeline.transformData(rawData, 'shopify');

      const order = transformedData.orders[0];
      const relatedItems = transformedData.orderItems.filter(
        item => item.source_order_id === order.source_id
      );

      assert.ok(relatedItems.length > 0);
      assert.equal(relatedItems[0].source_order_id, order.source_id);
    });

    it('should handle transformation errors', async () => {
      // Create invalid data that will cause transformation to fail
      const invalidData = {
        customers: [{ id: null }], // Invalid customer
        products: [],
        orders: [],
      };

      await assert.rejects(
        () => pipeline.transformData(invalidData, 'shopify'),
        /Error transforming customer/
      );
    });
  });

  describe('loadData', () => {
    const transformedData = {
      customers: [
        {
          source_id: '123',
          source_type: 'shopify',
          email: 'test@example.com',
        },
      ],
      products: [
        {
          source_id: '456',
          source_type: 'shopify',
          title: 'Test Product',
        },
      ],
      orders: [
        {
          source_id: '789',
          source_type: 'shopify',
          email: 'test@example.com',
          order_number: '1001',
        },
      ],
      orderItems: [
        {
          source_order_id: '789',
          source_product_id: '456',
          title: 'Test Product',
          quantity: 1,
          price: 10.00,
        },
      ],
    };

    it('should load all data types successfully', async () => {
      const results = await pipeline.loadData(transformedData, 'shopify');

      assert.equal(results.customers.inserted.length, 1);
      assert.equal(results.products.inserted.length, 1);
      assert.equal(results.orders.inserted.length, 1);
      assert.equal(results.orderItems.inserted.length, 1);
      assert.equal(results.totalLoaded, 4);
    });

    it('should link orders to customers by email', async () => {
      // Track calls to findCustomerId
      let customerLookups = [];
      pipeline.loader.findCustomerId = async function(email, sourceType) {
        customerLookups.push({ email, sourceType });
        return email === 'test@example.com' ? 123 : null;
      };

      await pipeline.loadData(transformedData, 'shopify');

      assert.ok(customerLookups.length > 0);
      assert.equal(customerLookups[0].email, 'test@example.com');
      assert.equal(customerLookups[0].sourceType, 'shopify');
    });

    it('should handle load failures gracefully', async () => {
      // Make customer upsert fail
      pipeline.loader.upsertCustomerBatch = async () => {
        throw new Error('Database connection failed');
      };

      await assert.rejects(
        () => pipeline.loadData(transformedData, 'shopify'),
        /Database connection failed/
      );
    });
  });

  describe('calculateMetrics', () => {
    it('should calculate customer and daily metrics', async () => {
      const transformedData = {
        customers: [{ source_id: '123' }],
        products: [{ source_id: '456', title: 'Test Product' }],
        orders: [],
        orderItems: [],
      };

      // Mock database queries for metrics calculation
      mockDatabase.setData('customers_with_orders', [
        {
          id: 1,
          orders: [
            {
              id: 1,
              customer_id: 1,
              total_price: 100,
              processed_at: new Date(),
            },
          ],
        },
      ]);

      mockDatabase.setData('daily_orders', [
        {
          id: 1,
          total_price: 100,
          line_items: [
            {
              source_product_id: '456',
              title: 'Test Product',
              quantity: 1,
              price: 100,
            },
          ],
        },
      ]);

      // Track metrics calls
      let metricsUpserted = [];
      pipeline.loader.upsertCustomerMetrics = async function(metrics) {
        metricsUpserted.push(metrics);
        return 1;
      };

      let dailyMetricsUpserted = [];
      pipeline.loader.upsertDailyMetrics = async function(metrics) {
        dailyMetricsUpserted.push(metrics);
        return 1;
      };

      await pipeline.calculateMetrics(transformedData, 'shopify');

      assert.ok(metricsUpserted.length > 0);
      assert.ok(dailyMetricsUpserted.length > 0);
      assert.equal(dailyMetricsUpserted[0].source_type, 'shopify');
    });

    it('should handle metrics calculation errors', async () => {
      const transformedData = { customers: [], products: [], orders: [], orderItems: [] };

      // Make metrics calculation fail
      pipeline.loader.pool.query = async () => {
        throw new Error('Query timeout');
      };

      await assert.rejects(
        () => pipeline.calculateMetrics(transformedData, 'shopify'),
        /Query timeout/
      );
    });
  });

  describe('updateCustomerPurchaseDates', () => {
    it('should update customer purchase dates', async () => {
      let updateQueries = [];
      pipeline.loader.pool.query = async function(sql, params) {
        updateQueries.push({ sql, params });
        return { rows: [] };
      };

      await pipeline.updateCustomerPurchaseDates('shopify');

      assert.ok(updateQueries.length > 0);
      const updateQuery = updateQueries[0];
      assert.ok(updateQuery.sql.includes('UPDATE customers'));
      assert.ok(updateQuery.sql.includes('first_purchase_date'));
      assert.ok(updateQuery.sql.includes('last_purchase_date'));
      assert.equal(updateQuery.params[0], 'shopify');
    });
  });

  describe('error handling', () => {
    it('should log errors and mark ETL as failed', async () => {
      // Make transformation fail
      pipeline.transformData = async () => {
        throw new Error('Transformation failed');
      };

      const result = await pipeline.runForSource('shopify');

      assert.equal(result.success, false);
      assert.equal(result.status, 'failed');
      assert.ok(result.error_message.includes('Transformation failed'));
    });

    it('should handle connector initialization errors', async () => {
      pipeline.connectors = {}; // No connectors

      const result = await pipeline.runForSource('shopify');

      assert.equal(result.success, false);
      assert.ok(result.error_message);
    });
  });

  describe('close', () => {
    it('should close database connections', async () => {
      let loaderClosed = false;
      pipeline.loader.close = async () => {
        loaderClosed = true;
      };

      await pipeline.close();

      assert.equal(loaderClosed, true);
    });
  });
});