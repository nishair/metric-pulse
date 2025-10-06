import axios from 'axios';
import { logger } from '../utils/logger.js';

export class ShopifyConnector {
  constructor(config) {
    this.storeUrl = config.storeUrl;
    this.accessToken = config.accessToken;
    this.apiVersion = '2024-01';
    this.baseURL = `${this.storeUrl}/admin/api/${this.apiVersion}`;
    this.rateLimitDelay = 500; // milliseconds between requests
  }

  async makeRequest(endpoint, params = {}) {
    try {
      const response = await axios.get(`${this.baseURL}${endpoint}`, {
        headers: {
          'X-Shopify-Access-Token': this.accessToken,
          'Content-Type': 'application/json',
        },
        params,
      });

      // Handle rate limiting
      if (response.headers['x-shopify-shop-api-call-limit']) {
        const [used, total] = response.headers['x-shopify-shop-api-call-limit'].split('/');
        if (parseInt(used) / parseInt(total) > 0.8) {
          await this.sleep(this.rateLimitDelay * 2);
        }
      }

      return response.data;
    } catch (error) {
      logger.error(`Shopify API error: ${error.message}`, {
        endpoint,
        status: error.response?.status,
        data: error.response?.data,
      });
      throw error;
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async *paginate(endpoint, params = {}) {
    let hasNextPage = true;
    let pageInfo = null;

    while (hasNextPage) {
      const queryParams = pageInfo
        ? { page_info: pageInfo, limit: params.limit || 250 }
        : { ...params, limit: params.limit || 250 };

      const response = await this.makeRequest(endpoint, queryParams);
      yield response;

      // Check for next page
      const linkHeader = response.headers?.link;
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const matches = linkHeader.match(/page_info=([^>]+).*?rel="next"/);
        pageInfo = matches ? matches[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await this.sleep(this.rateLimitDelay);
    }
  }

  async getCustomers(since = null) {
    const customers = [];
    const params = {};

    if (since) {
      params.updated_at_min = since.toISOString();
    }

    logger.info('Fetching Shopify customers', { since });

    for await (const batch of this.paginate('/customers.json', params)) {
      if (batch.customers) {
        customers.push(...batch.customers);
        logger.info(`Fetched ${batch.customers.length} customers`);
      }
    }

    return customers;
  }

  async getOrders(since = null) {
    const orders = [];
    const params = {
      status: 'any',
    };

    if (since) {
      params.updated_at_min = since.toISOString();
    }

    logger.info('Fetching Shopify orders', { since });

    for await (const batch of this.paginate('/orders.json', params)) {
      if (batch.orders) {
        orders.push(...batch.orders);
        logger.info(`Fetched ${batch.orders.length} orders`);
      }
    }

    return orders;
  }

  async getProducts(since = null) {
    const products = [];
    const params = {};

    if (since) {
      params.updated_at_min = since.toISOString();
    }

    logger.info('Fetching Shopify products', { since });

    for await (const batch of this.paginate('/products.json', params)) {
      if (batch.products) {
        products.push(...batch.products);
        logger.info(`Fetched ${batch.products.length} products`);
      }
    }

    return products;
  }

  async getInventoryLevels(locationIds = []) {
    const inventoryLevels = [];
    const params = {};

    if (locationIds.length > 0) {
      params.location_ids = locationIds.join(',');
    }

    logger.info('Fetching Shopify inventory levels');

    for await (const batch of this.paginate('/inventory_levels.json', params)) {
      if (batch.inventory_levels) {
        inventoryLevels.push(...batch.inventory_levels);
        logger.info(`Fetched ${batch.inventory_levels.length} inventory levels`);
      }
    }

    return inventoryLevels;
  }

  async getAnalytics() {
    try {
      const reports = {};

      // Get shop info for currency
      const shop = await this.makeRequest('/shop.json');
      reports.shop = shop.shop;

      // Get recent order statistics
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

      const recentOrders = await this.getOrders(thirtyDaysAgo);

      reports.orderStats = {
        totalOrders: recentOrders.length,
        totalRevenue: recentOrders.reduce((sum, order) => {
          const price = parseFloat(order.total_price || 0);
          return sum + price;
        }, 0),
        averageOrderValue: recentOrders.length > 0
          ? recentOrders.reduce((sum, order) => sum + parseFloat(order.total_price || 0), 0) / recentOrders.length
          : 0,
      };

      return reports;
    } catch (error) {
      logger.error('Failed to get analytics', error);
      throw error;
    }
  }

  async testConnection() {
    try {
      await this.makeRequest('/shop.json');
      logger.info('Shopify connection test successful');
      return true;
    } catch (error) {
      logger.error('Shopify connection test failed', error);
      return false;
    }
  }
}