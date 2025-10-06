import { logger } from '../utils/logger.js';

export class DataTransformer {
  constructor() {
    this.sourceType = null;
  }

  setSourceType(type) {
    this.sourceType = type;
  }

  transformCustomer(rawCustomer, sourceType) {
    try {
      const customer = {
        source_id: String(rawCustomer.id),
        source_type: sourceType,
        email: rawCustomer.email?.toLowerCase() || null,
        first_name: rawCustomer.first_name || null,
        last_name: rawCustomer.last_name || null,
        phone: this.extractPhone(rawCustomer),
        city: this.extractCity(rawCustomer),
        state: this.extractState(rawCustomer),
        country: this.extractCountry(rawCustomer),
        postal_code: this.extractPostalCode(rawCustomer),
        total_spent: parseFloat(rawCustomer.total_spent || 0),
        orders_count: parseInt(rawCustomer.orders_count || 0),
        tags: this.extractTags(rawCustomer),
        created_at: this.parseDate(rawCustomer.created_at),
        updated_at: this.parseDate(rawCustomer.updated_at),
      };

      return customer;
    } catch (error) {
      logger.error('Error transforming customer', { error, rawCustomer });
      throw error;
    }
  }

  transformProduct(rawProduct, sourceType) {
    try {
      const product = {
        source_id: String(rawProduct.id),
        source_type: sourceType,
        title: rawProduct.title || rawProduct.name,
        vendor: rawProduct.vendor || null,
        product_type: rawProduct.product_type || rawProduct.type || null,
        sku: this.extractSku(rawProduct),
        price: this.extractPrice(rawProduct),
        compare_at_price: this.extractComparePrice(rawProduct),
        inventory_quantity: this.extractInventory(rawProduct),
        tags: this.extractTags(rawProduct),
        status: this.normalizeStatus(rawProduct.status),
        created_at: this.parseDate(rawProduct.created_at),
        updated_at: this.parseDate(rawProduct.updated_at),
      };

      return product;
    } catch (error) {
      logger.error('Error transforming product', { error, rawProduct });
      throw error;
    }
  }

  transformOrder(rawOrder, sourceType) {
    try {
      const order = {
        source_id: String(rawOrder.id),
        source_type: sourceType,
        order_number: rawOrder.order_number || rawOrder.number || String(rawOrder.id),
        email: rawOrder.email?.toLowerCase() || rawOrder.contact_email?.toLowerCase() || null,
        financial_status: this.normalizeFinancialStatus(rawOrder.financial_status || rawOrder.status),
        fulfillment_status: rawOrder.fulfillment_status || null,
        currency: rawOrder.currency || 'USD',
        subtotal_price: parseFloat(rawOrder.subtotal_price || rawOrder.subtotal || 0),
        total_tax: parseFloat(rawOrder.total_tax || 0),
        total_discounts: parseFloat(rawOrder.total_discounts || rawOrder.discount_total || 0),
        total_shipping: parseFloat(rawOrder.total_shipping || rawOrder.shipping_total || 0),
        total_price: parseFloat(rawOrder.total_price || rawOrder.total || 0),
        processed_at: this.parseDate(rawOrder.processed_at || rawOrder.created_at),
        cancelled_at: this.parseDate(rawOrder.cancelled_at),
        tags: this.extractTags(rawOrder),
        source_name: rawOrder.source_name || null,
        created_at: this.parseDate(rawOrder.created_at),
        updated_at: this.parseDate(rawOrder.updated_at),
      };

      return order;
    } catch (error) {
      logger.error('Error transforming order', { error, rawOrder });
      throw error;
    }
  }

  transformOrderItem(rawItem, orderId, sourceType) {
    try {
      const item = {
        order_id: orderId,
        source_product_id: String(rawItem.product_id || ''),
        source_variant_id: String(rawItem.variant_id || rawItem.variation_id || ''),
        title: rawItem.title || rawItem.name,
        variant_title: rawItem.variant_title || null,
        sku: rawItem.sku || null,
        quantity: parseInt(rawItem.quantity || 1),
        price: parseFloat(rawItem.price || 0),
        total_discount: parseFloat(rawItem.total_discount || 0),
        fulfillment_status: rawItem.fulfillment_status || null,
      };

      return item;
    } catch (error) {
      logger.error('Error transforming order item', { error, rawItem });
      throw error;
    }
  }

  // Helper methods
  extractPhone(customer) {
    if (customer.phone) return customer.phone;
    if (customer.default_address?.phone) return customer.default_address.phone;
    if (customer.billing?.phone) return customer.billing.phone;
    return null;
  }

  extractCity(customer) {
    if (customer.default_address?.city) return customer.default_address.city;
    if (customer.addresses?.[0]?.city) return customer.addresses[0].city;
    if (customer.billing?.city) return customer.billing.city;
    return null;
  }

  extractState(customer) {
    if (customer.default_address?.province) return customer.default_address.province;
    if (customer.default_address?.province_code) return customer.default_address.province_code;
    if (customer.addresses?.[0]?.province) return customer.addresses[0].province;
    if (customer.billing?.state) return customer.billing.state;
    return null;
  }

  extractCountry(customer) {
    if (customer.default_address?.country) return customer.default_address.country;
    if (customer.default_address?.country_code) return customer.default_address.country_code;
    if (customer.addresses?.[0]?.country) return customer.addresses[0].country;
    if (customer.billing?.country) return customer.billing.country;
    return null;
  }

  extractPostalCode(customer) {
    if (customer.default_address?.zip) return customer.default_address.zip;
    if (customer.addresses?.[0]?.zip) return customer.addresses[0].zip;
    if (customer.billing?.postcode) return customer.billing.postcode;
    return null;
  }

  extractTags(entity) {
    if (!entity.tags) return [];
    if (Array.isArray(entity.tags)) {
      return entity.tags.map(tag => typeof tag === 'object' ? tag.name : tag);
    }
    if (typeof entity.tags === 'string') {
      return entity.tags.split(',').map(tag => tag.trim()).filter(Boolean);
    }
    return [];
  }

  extractSku(product) {
    if (product.sku) return product.sku;
    if (product.variants?.[0]?.sku) return product.variants[0].sku;
    return null;
  }

  extractPrice(product) {
    if (product.price) return parseFloat(product.price);
    if (product.variants?.[0]?.price) return parseFloat(product.variants[0].price);
    return 0;
  }

  extractComparePrice(product) {
    if (product.compare_at_price) return parseFloat(product.compare_at_price);
    if (product.regular_price) return parseFloat(product.regular_price);
    if (product.variants?.[0]?.compare_at_price) return parseFloat(product.variants[0].compare_at_price);
    return null;
  }

  extractInventory(product) {
    if (product.inventory_quantity !== undefined) return parseInt(product.inventory_quantity);
    if (product.stock_quantity !== undefined) return parseInt(product.stock_quantity);
    if (product.variants?.[0]?.inventory_quantity !== undefined) {
      return parseInt(product.variants[0].inventory_quantity);
    }
    return 0;
  }

  normalizeStatus(status) {
    if (!status) return 'active';
    const normalized = status.toLowerCase();
    if (['active', 'publish', 'published'].includes(normalized)) return 'active';
    if (['draft', 'inactive', 'unpublished'].includes(normalized)) return 'draft';
    if (['archived', 'deleted'].includes(normalized)) return 'archived';
    return status;
  }

  normalizeFinancialStatus(status) {
    if (!status) return 'pending';
    const normalized = status.toLowerCase();
    if (['paid', 'completed', 'complete'].includes(normalized)) return 'paid';
    if (['pending', 'processing', 'authorized'].includes(normalized)) return 'pending';
    if (['refunded', 'refund'].includes(normalized)) return 'refunded';
    if (['voided', 'cancelled', 'canceled'].includes(normalized)) return 'voided';
    if (['partially_paid', 'partial'].includes(normalized)) return 'partially_paid';
    if (['partially_refunded'].includes(normalized)) return 'partially_refunded';
    return status;
  }

  parseDate(dateString) {
    if (!dateString) return null;
    const date = new Date(dateString);
    return isNaN(date.getTime()) ? null : date;
  }

  // Batch transformation methods
  transformCustomerBatch(customers, sourceType) {
    return customers.map(customer => this.transformCustomer(customer, sourceType));
  }

  transformProductBatch(products, sourceType) {
    return products.map(product => this.transformProduct(product, sourceType));
  }

  transformOrderBatch(orders, sourceType) {
    return orders.map(order => this.transformOrder(order, sourceType));
  }

  // Data validation
  validateCustomer(customer) {
    const errors = [];
    if (!customer.source_id) errors.push('Missing source_id');
    if (!customer.source_type) errors.push('Missing source_type');
    if (customer.email && !this.isValidEmail(customer.email)) {
      errors.push('Invalid email format');
    }
    return { isValid: errors.length === 0, errors };
  }

  validateOrder(order) {
    const errors = [];
    if (!order.source_id) errors.push('Missing source_id');
    if (!order.source_type) errors.push('Missing source_type');
    if (order.total_price < 0) errors.push('Invalid total_price');
    return { isValid: errors.length === 0, errors };
  }

  validateProduct(product) {
    const errors = [];
    if (!product.source_id) errors.push('Missing source_id');
    if (!product.source_type) errors.push('Missing source_type');
    if (!product.title) errors.push('Missing title');
    if (product.price < 0) errors.push('Invalid price');
    return { isValid: errors.length === 0, errors };
  }

  isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }
}