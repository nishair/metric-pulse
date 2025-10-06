import { describe, it, beforeEach } from 'node:test';
import { strict as assert } from 'assert';
import { DataTransformer } from '../../src/transformers/dataTransformer.js';
import { shopifyCustomers, shopifyProducts, shopifyOrders } from '../fixtures/shopify-data.js';
import { transformedCustomer, transformedProduct, transformedOrder } from '../fixtures/transformed-data.js';
import { assertObjectPartialMatch, assertDateEquals } from '../utils/test-helpers.js';

describe('DataTransformer', () => {
  let transformer;

  beforeEach(() => {
    transformer = new DataTransformer();
  });

  describe('transformCustomer', () => {
    it('should transform Shopify customer correctly', () => {
      const result = transformer.transformCustomer(shopifyCustomers[0], 'shopify');

      assertObjectPartialMatch(result, {
        source_id: '123456789',
        source_type: 'shopify',
        email: 'john.doe@example.com',
        first_name: 'John',
        last_name: 'Doe',
        phone: '+1234567890',
        city: 'New York',
        state: 'NY',
        country: 'US',
        postal_code: '10001',
        total_spent: 1500.00,
        orders_count: 5,
      });

      assert.deepEqual(result.tags, ['VIP', 'Repeat Customer']);
      assertDateEquals(result.created_at, new Date('2023-01-15T10:00:00Z'));
      assertDateEquals(result.updated_at, new Date('2024-01-15T10:00:00Z'));
    });

    it('should handle customer with minimal data', () => {
      const minimalCustomer = {
        id: 999,
        email: 'minimal@example.com',
      };

      const result = transformer.transformCustomer(minimalCustomer, 'shopify');

      assert.equal(result.source_id, '999');
      assert.equal(result.source_type, 'shopify');
      assert.equal(result.email, 'minimal@example.com');
      assert.equal(result.first_name, null);
      assert.equal(result.total_spent, 0);
      assert.equal(result.orders_count, 0);
      assert.deepEqual(result.tags, []);
    });

    it('should normalize email to lowercase', () => {
      const customer = {
        id: 123,
        email: 'TEST@EXAMPLE.COM',
      };

      const result = transformer.transformCustomer(customer, 'shopify');
      assert.equal(result.email, 'test@example.com');
    });

    it('should handle invalid customer data gracefully', () => {
      const invalidCustomer = { id: null };

      assert.throws(
        () => transformer.transformCustomer(invalidCustomer, 'shopify'),
        /Error transforming customer/
      );
    });
  });

  describe('transformProduct', () => {
    it('should transform Shopify product correctly', () => {
      const result = transformer.transformProduct(shopifyProducts[0], 'shopify');

      assertObjectPartialMatch(result, {
        source_id: '111222333',
        source_type: 'shopify',
        title: 'Premium T-Shirt',
        vendor: 'Acme Clothing',
        product_type: 'Shirts',
        sku: 'PTS-L-001',
        price: 29.99,
        compare_at_price: 39.99,
        inventory_quantity: 100,
        status: 'active',
      });

      assert.deepEqual(result.tags, ['summer', 'cotton', 'bestseller']);
      assertDateEquals(result.created_at, new Date('2023-01-01T10:00:00Z'));
    });

    it('should handle product without variants', () => {
      const productWithoutVariants = {
        id: 999,
        title: 'Test Product',
        status: 'active',
      };

      const result = transformer.transformProduct(productWithoutVariants, 'shopify');

      assert.equal(result.source_id, '999');
      assert.equal(result.title, 'Test Product');
      assert.equal(result.sku, null);
      assert.equal(result.price, 0);
      assert.equal(result.inventory_quantity, 0);
    });

    it('should normalize product status', () => {
      const activeProduct = { id: 1, status: 'active' };
      const publishedProduct = { id: 2, status: 'publish' };
      const draftProduct = { id: 3, status: 'draft' };

      assert.equal(transformer.transformProduct(activeProduct, 'shopify').status, 'active');
      assert.equal(transformer.transformProduct(publishedProduct, 'shopify').status, 'active');
      assert.equal(transformer.transformProduct(draftProduct, 'shopify').status, 'draft');
    });
  });

  describe('transformOrder', () => {
    it('should transform Shopify order correctly', () => {
      const result = transformer.transformOrder(shopifyOrders[0], 'shopify');

      assertObjectPartialMatch(result, {
        source_id: '555666777',
        source_type: 'shopify',
        order_number: '1001',
        email: 'john.doe@example.com',
        financial_status: 'paid',
        fulfillment_status: 'fulfilled',
        currency: 'USD',
        subtotal_price: 109.98,
        total_tax: 10.00,
        total_discounts: 5.00,
        total_shipping: 10.00,
        total_price: 124.98,
        source_name: 'web',
      });

      assert.deepEqual(result.tags, ['online']);
      assertDateEquals(result.processed_at, new Date('2024-01-10T10:00:00Z'));
      assert.equal(result.cancelled_at, null);
    });

    it('should normalize financial status', () => {
      const paidOrder = { id: 1, financial_status: 'paid' };
      const completedOrder = { id: 2, status: 'completed' };
      const pendingOrder = { id: 3, financial_status: 'pending' };

      assert.equal(transformer.transformOrder(paidOrder, 'shopify').financial_status, 'paid');
      assert.equal(transformer.transformOrder(completedOrder, 'shopify').financial_status, 'paid');
      assert.equal(transformer.transformOrder(pendingOrder, 'shopify').financial_status, 'pending');
    });

    it('should handle missing email', () => {
      const orderWithoutEmail = {
        id: 999,
        total_price: '100.00',
      };

      const result = transformer.transformOrder(orderWithoutEmail, 'shopify');
      assert.equal(result.email, null);
    });
  });

  describe('transformOrderItem', () => {
    const mockOrderId = 123;
    const orderItem = shopifyOrders[0].line_items[0];

    it('should transform order item correctly', () => {
      const result = transformer.transformOrderItem(orderItem, mockOrderId, 'shopify');

      assertObjectPartialMatch(result, {
        order_id: mockOrderId,
        source_product_id: '111222333',
        source_variant_id: '444555666',
        title: 'Premium T-Shirt',
        variant_title: 'Large',
        sku: 'PTS-L-001',
        quantity: 2,
        price: 29.99,
        total_discount: 5.00,
        fulfillment_status: 'fulfilled',
      });
    });

    it('should handle minimal order item data', () => {
      const minimalItem = {
        title: 'Test Item',
        quantity: 1,
      };

      const result = transformer.transformOrderItem(minimalItem, mockOrderId, 'shopify');

      assert.equal(result.order_id, mockOrderId);
      assert.equal(result.title, 'Test Item');
      assert.equal(result.quantity, 1);
      assert.equal(result.price, 0);
      assert.equal(result.source_product_id, '');
    });
  });

  describe('batch transformation methods', () => {
    it('should transform customer batch', () => {
      const customers = transformer.transformCustomerBatch(shopifyCustomers, 'shopify');

      assert.equal(customers.length, 2);
      assert.equal(customers[0].email, 'john.doe@example.com');
      assert.equal(customers[1].email, 'jane.smith@example.com');
    });

    it('should transform product batch', () => {
      const products = transformer.transformProductBatch(shopifyProducts, 'shopify');

      assert.equal(products.length, 2);
      assert.equal(products[0].title, 'Premium T-Shirt');
      assert.equal(products[1].title, 'Classic Jeans');
    });

    it('should transform order batch', () => {
      const orders = transformer.transformOrderBatch(shopifyOrders, 'shopify');

      assert.equal(orders.length, 2);
      assert.equal(orders[0].order_number, '1001');
      assert.equal(orders[1].order_number, '1002');
    });
  });

  describe('helper methods', () => {
    describe('extractTags', () => {
      it('should handle string tags', () => {
        const entity = { tags: 'VIP, Premium, Repeat Customer' };
        const result = transformer.extractTags(entity);
        assert.deepEqual(result, ['VIP', 'Premium', 'Repeat Customer']);
      });

      it('should handle array tags', () => {
        const entity = { tags: ['VIP', 'Premium'] };
        const result = transformer.extractTags(entity);
        assert.deepEqual(result, ['VIP', 'Premium']);
      });

      it('should handle object array tags', () => {
        const entity = { tags: [{ name: 'VIP' }, { name: 'Premium' }] };
        const result = transformer.extractTags(entity);
        assert.deepEqual(result, ['VIP', 'Premium']);
      });

      it('should handle empty tags', () => {
        const entity = { tags: '' };
        const result = transformer.extractTags(entity);
        assert.deepEqual(result, []);
      });
    });

    describe('parseDate', () => {
      it('should parse valid date string', () => {
        const result = transformer.parseDate('2023-01-15T10:00:00Z');
        assertDateEquals(result, new Date('2023-01-15T10:00:00Z'));
      });

      it('should return null for invalid date', () => {
        const result = transformer.parseDate('invalid-date');
        assert.equal(result, null);
      });

      it('should return null for null input', () => {
        const result = transformer.parseDate(null);
        assert.equal(result, null);
      });
    });

    describe('isValidEmail', () => {
      it('should validate correct email', () => {
        assert.equal(transformer.isValidEmail('test@example.com'), true);
        assert.equal(transformer.isValidEmail('user.name+tag@domain.co.uk'), true);
      });

      it('should reject invalid email', () => {
        assert.equal(transformer.isValidEmail('invalid-email'), false);
        assert.equal(transformer.isValidEmail('test@'), false);
        assert.equal(transformer.isValidEmail('@example.com'), false);
      });
    });
  });

  describe('data validation', () => {
    it('should validate customer data', () => {
      const validCustomer = transformedCustomer;
      const result = transformer.validateCustomer(validCustomer);

      assert.equal(result.isValid, true);
      assert.equal(result.errors.length, 0);
    });

    it('should catch customer validation errors', () => {
      const invalidCustomer = {
        source_type: 'shopify',
        email: 'invalid-email',
      };

      const result = transformer.validateCustomer(invalidCustomer);

      assert.equal(result.isValid, false);
      assert.ok(result.errors.includes('Missing source_id'));
      assert.ok(result.errors.includes('Invalid email format'));
    });

    it('should validate order data', () => {
      const validOrder = transformedOrder;
      const result = transformer.validateOrder(validOrder);

      assert.equal(result.isValid, true);
      assert.equal(result.errors.length, 0);
    });

    it('should catch order validation errors', () => {
      const invalidOrder = {
        source_type: 'shopify',
        total_price: -100,
      };

      const result = transformer.validateOrder(invalidOrder);

      assert.equal(result.isValid, false);
      assert.ok(result.errors.includes('Missing source_id'));
      assert.ok(result.errors.includes('Invalid total_price'));
    });

    it('should validate product data', () => {
      const validProduct = transformedProduct;
      const result = transformer.validateProduct(validProduct);

      assert.equal(result.isValid, true);
      assert.equal(result.errors.length, 0);
    });

    it('should catch product validation errors', () => {
      const invalidProduct = {
        source_id: '123',
        price: -10,
      };

      const result = transformer.validateProduct(invalidProduct);

      assert.equal(result.isValid, false);
      assert.ok(result.errors.includes('Missing source_type'));
      assert.ok(result.errors.includes('Missing title'));
      assert.ok(result.errors.includes('Invalid price'));
    });
  });
});