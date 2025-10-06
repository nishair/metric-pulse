import { strict as assert } from 'assert';

export class MockConnector {
  constructor(data = {}) {
    this.data = data;
    this.connectionTestResult = true;
  }

  async testConnection() {
    return this.connectionTestResult;
  }

  async getCustomers(since = null) {
    return this.data.customers || [];
  }

  async getOrders(since = null) {
    return this.data.orders || [];
  }

  async getProducts(since = null) {
    return this.data.products || [];
  }

  setConnectionTestResult(result) {
    this.connectionTestResult = result;
  }
}

export class MockDatabase {
  constructor() {
    this.data = {};
    this.queries = [];
    this.shouldFail = false;
    this.failureMessage = 'Mock database error';
  }

  async query(sql, params = []) {
    this.queries.push({ sql, params });

    if (this.shouldFail) {
      throw new Error(this.failureMessage);
    }

    // Mock different query responses
    if (sql.includes('INSERT') && sql.includes('RETURNING id')) {
      return { rows: [{ id: Math.floor(Math.random() * 1000) }] };
    }

    if (sql.includes('SELECT') && sql.includes('customers')) {
      return { rows: this.data.customers || [] };
    }

    if (sql.includes('SELECT') && sql.includes('orders')) {
      return { rows: this.data.orders || [] };
    }

    return { rows: [] };
  }

  connect() {
    return Promise.resolve(this);
  }

  release() {
    return Promise.resolve();
  }

  setData(tableName, data) {
    this.data[tableName] = data;
  }

  getQueries() {
    return this.queries;
  }

  clearQueries() {
    this.queries = [];
  }

  setShouldFail(shouldFail, message = 'Mock database error') {
    this.shouldFail = shouldFail;
    this.failureMessage = message;
  }
}

export function createMockLogger() {
  const logs = {
    info: [],
    error: [],
    warn: [],
    debug: [],
  };

  return {
    info: (...args) => logs.info.push(args),
    error: (...args) => logs.error.push(args),
    warn: (...args) => logs.warn.push(args),
    debug: (...args) => logs.debug.push(args),
    getLogs: () => logs,
    clearLogs: () => {
      logs.info = [];
      logs.error = [];
      logs.warn = [];
      logs.debug = [];
    },
  };
}

export function assertDateEquals(actual, expected, message = 'Dates should be equal') {
  if (actual instanceof Date && expected instanceof Date) {
    assert.equal(actual.getTime(), expected.getTime(), message);
  } else {
    assert.equal(actual, expected, message);
  }
}

export function assertArrayContains(array, item, message = 'Array should contain item') {
  const found = array.some(element => {
    if (typeof element === 'object' && typeof item === 'object') {
      return JSON.stringify(element) === JSON.stringify(item);
    }
    return element === item;
  });
  assert.ok(found, message);
}

export function assertObjectPartialMatch(actual, expected, message = 'Objects should partially match') {
  for (const [key, value] of Object.entries(expected)) {
    if (value instanceof Date && actual[key] instanceof Date) {
      assert.equal(actual[key].getTime(), value.getTime(), `${message} - field: ${key}`);
    } else if (Array.isArray(value)) {
      assert.deepEqual(actual[key], value, `${message} - field: ${key}`);
    } else {
      assert.equal(actual[key], value, `${message} - field: ${key}`);
    }
  }
}

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function createTestConfig() {
  return {
    shopify: {
      enabled: true,
      storeUrl: 'https://test-store.myshopify.com',
      accessToken: 'test-token',
    },
    woocommerce: {
      enabled: false,
      url: 'https://test-woo.com',
      consumerKey: 'test-key',
      consumerSecret: 'test-secret',
    },
    commercetools: {
      enabled: false,
      projectKey: 'test-project',
      clientId: 'test-client',
      clientSecret: 'test-secret',
      region: 'us-central1',
    },
    database: {
      host: 'localhost',
      port: 5432,
      database: 'test_ecommerce_analytics',
      user: 'test_user',
      password: 'test_password',
    },
    pipeline: {
      scheduleCron: '0 2 * * *',
      batchSize: 10,
      retryAttempts: 1,
      retryDelay: 100,
    },
  };
}