import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'assert';
import { ShopifyConnector } from '../../src/connectors/shopify.js';
import { shopifyCustomers, shopifyProducts, shopifyOrders } from '../fixtures/shopify-data.js';

// Mock axios
let mockAxios = {
  responses: new Map(),
  requests: [],
  get: async function(url, config) {
    this.requests.push({ url, config });

    if (this.responses.has(url)) {
      const response = this.responses.get(url);
      if (response instanceof Error) {
        throw response;
      }
      return response;
    }

    // Default responses
    if (url.includes('/customers.json')) {
      return {
        data: { customers: shopifyCustomers },
        headers: {}
      };
    }

    if (url.includes('/products.json')) {
      return {
        data: { products: shopifyProducts },
        headers: {}
      };
    }

    if (url.includes('/orders.json')) {
      return {
        data: { orders: shopifyOrders },
        headers: {}
      };
    }

    if (url.includes('/shop.json')) {
      return {
        data: {
          shop: {
            name: 'Test Store',
            currency: 'USD',
            timezone: 'America/New_York'
          }
        }
      };
    }

    return { data: {} };
  },

  setResponse: function(url, response) {
    this.responses.set(url, response);
  },

  clearResponses: function() {
    this.responses.clear();
    this.requests = [];
  }
};

// Mock axios module
const originalAxios = await import('axios');
originalAxios.default.get = mockAxios.get.bind(mockAxios);

describe('ShopifyConnector', () => {
  let connector;
  const config = {
    storeUrl: 'https://test-store.myshopify.com',
    accessToken: 'test-token'
  };

  beforeEach(() => {
    connector = new ShopifyConnector(config);
    mockAxios.clearResponses();
  });

  afterEach(() => {
    mockAxios.clearResponses();
  });

  describe('constructor', () => {
    it('should initialize with correct configuration', () => {
      assert.equal(connector.storeUrl, config.storeUrl);
      assert.equal(connector.accessToken, config.accessToken);
      assert.equal(connector.apiVersion, '2024-01');
      assert.equal(connector.rateLimitDelay, 500);
    });
  });

  describe('makeRequest', () => {
    it('should make request with correct headers', async () => {
      await connector.makeRequest('/test');

      const request = mockAxios.requests[0];
      assert.ok(request.config.headers['X-Shopify-Access-Token'] === 'test-token');
      assert.ok(request.config.headers['Content-Type'] === 'application/json');
    });

    it('should handle rate limiting headers', async () => {
      mockAxios.setResponse('https://test-store.myshopify.com/admin/api/2024-01/test', {
        data: { test: true },
        headers: {
          'x-shopify-shop-api-call-limit': '35/40'
        }
      });

      const result = await connector.makeRequest('/test');
      assert.deepEqual(result, { test: true });
    });

    it('should throw error on API failure', async () => {
      const error = new Error('API Error');
      error.response = {
        status: 401,
        data: { error: 'Unauthorized' }
      };

      mockAxios.setResponse('https://test-store.myshopify.com/admin/api/2024-01/test', error);

      await assert.rejects(
        () => connector.makeRequest('/test'),
        /API Error/
      );
    });
  });

  describe('getCustomers', () => {
    it('should fetch customers without date filter', async () => {
      const customers = await connector.getCustomers();

      assert.equal(customers.length, 2);
      assert.equal(customers[0].email, 'john.doe@example.com');
      assert.equal(customers[1].email, 'jane.smith@example.com');
    });

    it('should fetch customers with date filter', async () => {
      const since = new Date('2023-06-01');
      await connector.getCustomers(since);

      const request = mockAxios.requests[0];
      assert.ok(request.config.params.updated_at_min === since.toISOString());
    });

    it('should handle empty response', async () => {
      mockAxios.setResponse('https://test-store.myshopify.com/admin/api/2024-01/customers.json', {
        data: { customers: [] },
        headers: {}
      });

      const customers = await connector.getCustomers();
      assert.equal(customers.length, 0);
    });
  });

  describe('getProducts', () => {
    it('should fetch products', async () => {
      const products = await connector.getProducts();

      assert.equal(products.length, 2);
      assert.equal(products[0].title, 'Premium T-Shirt');
      assert.equal(products[1].title, 'Classic Jeans');
    });

    it('should include date filter when provided', async () => {
      const since = new Date('2023-01-01');
      await connector.getProducts(since);

      const request = mockAxios.requests[0];
      assert.ok(request.config.params.updated_at_min === since.toISOString());
    });
  });

  describe('getOrders', () => {
    it('should fetch orders with correct parameters', async () => {
      const orders = await connector.getOrders();

      assert.equal(orders.length, 2);
      assert.equal(orders[0].order_number, '1001');

      const request = mockAxios.requests[0];
      assert.equal(request.config.params.status, 'any');
    });

    it('should handle date filtering', async () => {
      const since = new Date('2024-01-01');
      await connector.getOrders(since);

      const request = mockAxios.requests[0];
      assert.ok(request.config.params.updated_at_min === since.toISOString());
    });
  });

  describe('getAnalytics', () => {
    it('should fetch analytics data', async () => {
      const analytics = await connector.getAnalytics();

      assert.ok(analytics.shop);
      assert.equal(analytics.shop.name, 'Test Store');
      assert.ok(analytics.orderStats);
      assert.ok(typeof analytics.orderStats.totalOrders === 'number');
      assert.ok(typeof analytics.orderStats.totalRevenue === 'number');
    });

    it('should calculate order statistics correctly', async () => {
      const analytics = await connector.getAnalytics();

      // Should calculate based on recent orders (30 days)
      assert.equal(analytics.orderStats.totalOrders, 2);
      assert.equal(analytics.orderStats.totalRevenue, 162.67); // 124.98 + 37.69
    });
  });

  describe('testConnection', () => {
    it('should return true on successful connection', async () => {
      const result = await connector.testConnection();
      assert.equal(result, true);
    });

    it('should return false on connection failure', async () => {
      const error = new Error('Connection failed');
      mockAxios.setResponse('https://test-store.myshopify.com/admin/api/2024-01/shop.json', error);

      const result = await connector.testConnection();
      assert.equal(result, false);
    });
  });

  describe('pagination', () => {
    it('should handle pagination correctly', async () => {
      // Mock paginated response
      let callCount = 0;
      const originalGet = mockAxios.get;

      mockAxios.get = async function(url, config) {
        callCount++;

        if (callCount === 1) {
          return {
            data: { customers: [shopifyCustomers[0]] },
            headers: {
              link: '<https://test-store.myshopify.com/admin/api/2024-01/customers.json?page_info=abc123>; rel="next"'
            }
          };
        } else {
          return {
            data: { customers: [shopifyCustomers[1]] },
            headers: {}
          };
        }
      };

      const customers = await connector.getCustomers();

      assert.equal(customers.length, 2);
      assert.equal(callCount, 2);

      // Restore original mock
      mockAxios.get = originalGet.bind(mockAxios);
    });
  });

  describe('sleep', () => {
    it('should wait for specified duration', async () => {
      const start = Date.now();
      await connector.sleep(50);
      const end = Date.now();

      assert.ok(end - start >= 45); // Allow some variance
    });
  });
});