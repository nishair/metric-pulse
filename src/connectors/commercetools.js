import axios from 'axios';
import { logger } from '../utils/logger.js';

export class CommercetoolsConnector {
  constructor(config) {
    this.projectKey = config.projectKey;
    this.clientId = config.clientId;
    this.clientSecret = config.clientSecret;
    this.apiUrl = config.apiUrl || `https://api.${config.region}.commercetools.com`;
    this.authUrl = config.authUrl || `https://auth.${config.region}.commercetools.com`;
    this.scope = config.scope || `manage_project:${this.projectKey}`;
    this.accessToken = null;
    this.tokenExpiry = null;
    this.rateLimitDelay = 200; // milliseconds between requests
  }

  async getAccessToken() {
    // Check if we have a valid token
    if (this.accessToken && this.tokenExpiry && new Date() < this.tokenExpiry) {
      return this.accessToken;
    }

    try {
      const response = await axios.post(
        `${this.authUrl}/oauth/token`,
        `grant_type=client_credentials&scope=${this.scope}`,
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': `Basic ${Buffer.from(`${this.clientId}:${this.clientSecret}`).toString('base64')}`,
          },
        }
      );

      this.accessToken = response.data.access_token;
      // Set expiry 5 minutes before actual expiry for safety
      this.tokenExpiry = new Date(Date.now() + (response.data.expires_in - 300) * 1000);

      logger.info('Commercetools access token obtained');
      return this.accessToken;
    } catch (error) {
      logger.error('Failed to get Commercetools access token', error);
      throw error;
    }
  }

  async makeRequest(endpoint, params = {}) {
    try {
      const accessToken = await this.getAccessToken();

      const response = await axios.get(
        `${this.apiUrl}/${this.projectKey}${endpoint}`,
        {
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
          },
          params,
        }
      );

      return response.data;
    } catch (error) {
      logger.error(`Commercetools API error: ${error.message}`, {
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
    const limit = params.limit || 500;
    let offset = 0;
    let hasMorePages = true;

    while (hasMorePages) {
      const response = await this.makeRequest(endpoint, {
        ...params,
        limit,
        offset,
      });

      yield response;

      // Check if there are more pages
      hasMorePages = response.total > offset + limit;
      offset += limit;

      await this.sleep(this.rateLimitDelay);
    }
  }

  async getCustomers(since = null) {
    const customers = [];
    const params = {
      sort: 'lastModifiedAt asc',
    };

    if (since) {
      params.where = `lastModifiedAt > "${since.toISOString()}"`;
    }

    logger.info('Fetching Commercetools customers', { since });

    for await (const batch of this.paginate('/customers', params)) {
      if (batch.results) {
        customers.push(...batch.results);
        logger.info(`Fetched ${batch.results.length} customers`);
      }
    }

    return customers;
  }

  async getOrders(since = null) {
    const orders = [];
    const params = {
      sort: 'lastModifiedAt asc',
      expand: ['customer', 'lineItems[*].productType', 'lineItems[*].variant'],
    };

    if (since) {
      params.where = `lastModifiedAt > "${since.toISOString()}"`;
    }

    logger.info('Fetching Commercetools orders', { since });

    for await (const batch of this.paginate('/orders', params)) {
      if (batch.results) {
        orders.push(...batch.results);
        logger.info(`Fetched ${batch.results.length} orders`);
      }
    }

    return orders;
  }

  async getProducts(since = null) {
    const products = [];
    const params = {
      sort: 'lastModifiedAt asc',
      expand: ['productType', 'masterVariant.prices[*]'],
    };

    if (since) {
      params.where = `lastModifiedAt > "${since.toISOString()}"`;
    }

    logger.info('Fetching Commercetools products', { since });

    for await (const batch of this.paginate('/products', params)) {
      if (batch.results) {
        products.push(...batch.results);
        logger.info(`Fetched ${batch.results.length} products`);
      }
    }

    return products;
  }

  async getInventory(since = null) {
    const inventory = [];
    const params = {
      sort: 'lastModifiedAt asc',
    };

    if (since) {
      params.where = `lastModifiedAt > "${since.toISOString()}"`;
    }

    logger.info('Fetching Commercetools inventory');

    for await (const batch of this.paginate('/inventory', params)) {
      if (batch.results) {
        inventory.push(...batch.results);
        logger.info(`Fetched ${batch.results.length} inventory entries`);
      }
    }

    return inventory;
  }

  async getCarts(since = null) {
    const carts = [];
    const params = {
      sort: 'lastModifiedAt asc',
    };

    if (since) {
      params.where = `lastModifiedAt > "${since.toISOString()}"`;
    }

    logger.info('Fetching Commercetools carts');

    for await (const batch of this.paginate('/carts', params)) {
      if (batch.results) {
        carts.push(...batch.results);
        logger.info(`Fetched ${batch.results.length} carts`);
      }
    }

    return carts;
  }

  async getAnalytics() {
    try {
      const analytics = {};

      // Get project info
      const project = await this.makeRequest('');
      analytics.project = {
        name: project.name,
        currencies: project.currencies,
        languages: project.languages,
      };

      // Get order statistics for the last 30 days
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

      const ordersResponse = await this.makeRequest('/orders', {
        where: `createdAt > "${thirtyDaysAgo.toISOString()}"`,
        limit: 1,
      });

      analytics.recentOrderCount = ordersResponse.total || 0;

      // Get customer count
      const customersResponse = await this.makeRequest('/customers', { limit: 1 });
      analytics.totalCustomers = customersResponse.total || 0;

      // Get product count
      const productsResponse = await this.makeRequest('/products', { limit: 1 });
      analytics.totalProducts = productsResponse.total || 0;

      return analytics;
    } catch (error) {
      logger.error('Failed to get analytics', error);
      return {};
    }
  }

  async testConnection() {
    try {
      await this.getAccessToken();
      const project = await this.makeRequest('');
      logger.info('Commercetools connection test successful', { project: project.key });
      return true;
    } catch (error) {
      logger.error('Commercetools connection test failed', error);
      return false;
    }
  }

  // Transform Commercetools data to match our unified structure
  transformCustomer(customer) {
    return {
      id: customer.id,
      email: customer.email,
      first_name: customer.firstName,
      last_name: customer.lastName,
      phone: customer.addresses?.[0]?.phone || '',
      addresses: customer.addresses?.map(addr => ({
        city: addr.city,
        province: addr.state,
        country: addr.country,
        zip: addr.postalCode,
      })) || [],
      total_spent: 0, // Will be calculated from orders
      orders_count: 0, // Will be calculated from orders
      created_at: customer.createdAt,
      updated_at: customer.lastModifiedAt,
      tags: customer.custom?.fields?.tags || [],
    };
  }

  transformOrder(order) {
    const totalPrice = order.totalPrice?.centAmount
      ? order.totalPrice.centAmount / 100
      : 0;

    const subtotal = order.taxedPrice?.totalNet?.centAmount
      ? order.taxedPrice.totalNet.centAmount / 100
      : totalPrice;

    const totalTax = order.taxedPrice?.totalTax?.centAmount
      ? order.taxedPrice.totalTax.centAmount / 100
      : 0;

    const shippingTotal = order.shippingInfo?.price?.centAmount
      ? order.shippingInfo.price.centAmount / 100
      : 0;

    const discountTotal = order.discountCodes?.reduce((sum, dc) => {
      const amount = dc.discountedAmount?.centAmount || 0;
      return sum + (amount / 100);
    }, 0) || 0;

    return {
      id: order.id,
      order_number: order.orderNumber || order.id,
      email: order.customerEmail,
      financial_status: this.mapOrderState(order.orderState),
      fulfillment_status: order.shipmentState,
      total_price: totalPrice,
      subtotal_price: subtotal,
      total_tax: totalTax,
      total_shipping: shippingTotal,
      total_discounts: discountTotal,
      currency: order.totalPrice?.currencyCode || 'USD',
      created_at: order.createdAt,
      updated_at: order.lastModifiedAt,
      line_items: order.lineItems?.map(item => ({
        id: item.id,
        product_id: item.productId,
        variant_id: item.variant?.id,
        title: item.name?.[Object.keys(item.name)[0]] || '',
        quantity: item.quantity,
        price: item.price?.value?.centAmount ? item.price.value.centAmount / 100 : 0,
        sku: item.variant?.sku,
      })) || [],
    };
  }

  transformProduct(product) {
    const masterVariant = product.masterData?.current?.masterVariant || {};
    const productData = product.masterData?.current || {};

    const price = masterVariant.prices?.[0]?.value?.centAmount
      ? masterVariant.prices[0].value.centAmount / 100
      : 0;

    const name = productData.name
      ? productData.name[Object.keys(productData.name)[0]]
      : '';

    return {
      id: product.id,
      title: name,
      vendor: product.productType?.obj?.name || '',
      product_type: product.productType?.id || '',
      status: product.masterData?.published ? 'active' : 'draft',
      tags: productData.categories?.map(cat => cat.id) || [],
      variants: [{
        id: masterVariant.id,
        product_id: product.id,
        title: name,
        price: price,
        sku: masterVariant.sku,
        inventory_quantity: 0, // Will be updated from inventory data
      }],
      created_at: product.createdAt,
      updated_at: product.lastModifiedAt,
    };
  }

  mapOrderState(state) {
    const stateMap = {
      'Open': 'pending',
      'Confirmed': 'pending',
      'Complete': 'paid',
      'Cancelled': 'voided',
    };

    return stateMap[state] || state?.toLowerCase() || 'pending';
  }
}