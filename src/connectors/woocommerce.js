import axios from 'axios';
import { logger } from '../utils/logger.js';

export class WooCommerceConnector {
  constructor(config) {
    this.baseURL = config.url;
    this.consumerKey = config.consumerKey;
    this.consumerSecret = config.consumerSecret;
    this.version = 'wc/v3';
    this.rateLimitDelay = 300; // milliseconds between requests
  }

  async makeRequest(endpoint, params = {}) {
    try {
      const response = await axios.get(`${this.baseURL}/wp-json/${this.version}${endpoint}`, {
        auth: {
          username: this.consumerKey,
          password: this.consumerSecret,
        },
        params,
      });

      return response;
    } catch (error) {
      logger.error(`WooCommerce API error: ${error.message}`, {
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
    let page = 1;
    let hasMorePages = true;

    while (hasMorePages) {
      const response = await this.makeRequest(endpoint, {
        ...params,
        page,
        per_page: params.per_page || 100,
      });

      yield response.data;

      // Check if there are more pages
      const totalPages = parseInt(response.headers['x-wp-totalpages'] || 1);
      hasMorePages = page < totalPages;
      page++;

      await this.sleep(this.rateLimitDelay);
    }
  }

  async getCustomers(since = null) {
    const customers = [];
    const params = {
      orderby: 'registered_date',
      order: 'asc',
    };

    if (since) {
      params.after = since.toISOString();
    }

    logger.info('Fetching WooCommerce customers', { since });

    for await (const batch of this.paginate('/customers', params)) {
      customers.push(...batch);
      logger.info(`Fetched ${batch.length} customers`);
    }

    return customers;
  }

  async getOrders(since = null) {
    const orders = [];
    const params = {
      orderby: 'date',
      order: 'asc',
      status: 'any',
    };

    if (since) {
      params.after = since.toISOString();
    }

    logger.info('Fetching WooCommerce orders', { since });

    for await (const batch of this.paginate('/orders', params)) {
      orders.push(...batch);
      logger.info(`Fetched ${batch.length} orders`);
    }

    return orders;
  }

  async getProducts(since = null) {
    const products = [];
    const params = {
      orderby: 'date',
      order: 'asc',
      status: 'any',
    };

    if (since) {
      params.after = since.toISOString();
    }

    logger.info('Fetching WooCommerce products', { since });

    for await (const batch of this.paginate('/products', params)) {
      products.push(...batch);
      logger.info(`Fetched ${batch.length} products`);
    }

    return products;
  }

  async getProductVariations(productId) {
    const variations = [];

    logger.info(`Fetching variations for product ${productId}`);

    for await (const batch of this.paginate(`/products/${productId}/variations`, {})) {
      variations.push(...batch);
    }

    return variations;
  }

  async getReports(type = 'sales') {
    try {
      const response = await this.makeRequest(`/reports/${type}`, {
        period: 'month',
      });

      return response.data;
    } catch (error) {
      logger.error('Failed to get reports', error);
      throw error;
    }
  }

  async getAnalytics() {
    try {
      const reports = {};

      // Get store system status for basic info
      const systemStatus = await this.makeRequest('/system_status');
      reports.store = {
        currency: systemStatus.data.settings?.currency || 'USD',
        timezone: systemStatus.data.settings?.timezone_string,
      };

      // Get sales report
      const salesReport = await this.getReports('sales');
      reports.sales = salesReport;

      // Get top sellers report
      const topSellers = await this.getReports('top_sellers');
      reports.topSellers = topSellers;

      return reports;
    } catch (error) {
      logger.error('Failed to get analytics', error);
      return {};
    }
  }

  async getCoupons() {
    const coupons = [];

    logger.info('Fetching WooCommerce coupons');

    for await (const batch of this.paginate('/coupons', {})) {
      coupons.push(...batch);
      logger.info(`Fetched ${batch.length} coupons`);
    }

    return coupons;
  }

  async testConnection() {
    try {
      const response = await this.makeRequest('/system_status');
      logger.info('WooCommerce connection test successful');
      return true;
    } catch (error) {
      logger.error('WooCommerce connection test failed', error);
      return false;
    }
  }

  // Transform WooCommerce data to match Shopify structure for consistency
  transformCustomer(customer) {
    return {
      id: customer.id?.toString(),
      email: customer.email,
      first_name: customer.first_name,
      last_name: customer.last_name,
      phone: customer.billing?.phone || '',
      addresses: [
        {
          city: customer.billing?.city,
          province: customer.billing?.state,
          country: customer.billing?.country,
          zip: customer.billing?.postcode,
        }
      ],
      total_spent: customer.total_spent,
      orders_count: customer.orders_count,
      created_at: customer.date_created,
      updated_at: customer.date_modified,
    };
  }

  transformOrder(order) {
    return {
      id: order.id?.toString(),
      order_number: order.number,
      email: order.billing?.email,
      financial_status: order.status === 'completed' ? 'paid' : order.status,
      total_price: order.total,
      subtotal_price: order.subtotal || order.total,
      total_tax: order.total_tax,
      total_shipping: order.shipping_total,
      total_discounts: order.discount_total,
      currency: order.currency,
      created_at: order.date_created,
      updated_at: order.date_modified,
      line_items: order.line_items?.map(item => ({
        id: item.id?.toString(),
        product_id: item.product_id?.toString(),
        variant_id: item.variation_id?.toString(),
        title: item.name,
        quantity: item.quantity,
        price: item.price,
        sku: item.sku,
      })) || [],
    };
  }

  transformProduct(product) {
    return {
      id: product.id?.toString(),
      title: product.name,
      vendor: product.brands?.[0] || '',
      product_type: product.type,
      status: product.status === 'publish' ? 'active' : product.status,
      tags: product.tags?.map(tag => tag.name) || [],
      variants: [{
        id: product.id?.toString(),
        product_id: product.id?.toString(),
        title: product.name,
        price: product.price,
        compare_at_price: product.regular_price,
        sku: product.sku,
        inventory_quantity: product.stock_quantity,
      }],
      created_at: product.date_created,
      updated_at: product.date_modified,
    };
  }
}