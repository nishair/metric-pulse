import { describe, it, beforeEach } from 'node:test';
import { strict as assert } from 'assert';
import { CLVCalculator } from '../../src/analytics/clvCalculator.js';
import { assertObjectPartialMatch } from '../utils/test-helpers.js';

describe('CLVCalculator', () => {
  let calculator;

  beforeEach(() => {
    calculator = new CLVCalculator();
  });

  describe('constructor', () => {
    it('should initialize with default values', () => {
      assert.equal(calculator.defaultChurnDays, 365);
      assert.deepEqual(calculator.rfmWeights, {
        recency: 0.35,
        frequency: 0.35,
        monetary: 0.30,
      });
    });
  });

  describe('calculateCustomerMetrics', () => {
    const mockCustomer = {
      id: 1,
      email: 'test@example.com',
      first_name: 'John',
      last_name: 'Doe',
    };

    const mockOrders = [
      {
        id: 1,
        customer_id: 1,
        total_price: 100.00,
        processed_at: new Date('2024-01-01'),
      },
      {
        id: 2,
        customer_id: 1,
        total_price: 150.00,
        processed_at: new Date('2024-01-15'),
      },
      {
        id: 3,
        customer_id: 1,
        total_price: 200.00,
        processed_at: new Date('2024-02-01'),
      },
    ];

    it('should calculate basic metrics correctly', () => {
      const calculationDate = new Date('2024-03-01');
      const result = calculator.calculateCustomerMetrics(mockCustomer, mockOrders, calculationDate);

      assertObjectPartialMatch(result, {
        customer_id: 1,
        total_revenue: 450.00,
        total_orders: 3,
        average_order_value: 150.00,
      });

      assert.ok(result.customer_lifespan_days >= 31); // At least 31 days between first and last order
      assert.ok(result.purchase_frequency > 0);
      assert.ok(result.customer_lifetime_value > 0);
    });

    it('should calculate days since last purchase', () => {
      const calculationDate = new Date('2024-02-15'); // 14 days after last order
      const result = calculator.calculateCustomerMetrics(mockCustomer, mockOrders, calculationDate);

      assert.equal(result.days_since_last_purchase, 14);
    });

    it('should return empty metrics for customer with no orders', () => {
      const result = calculator.calculateCustomerMetrics(mockCustomer, [], new Date());

      assertObjectPartialMatch(result, {
        customer_id: 1,
        total_revenue: 0,
        total_orders: 0,
        average_order_value: 0,
        purchase_frequency: 0,
        customer_lifetime_value: 0,
        churn_probability: 1,
        customer_segment: 'Inactive',
      });
    });

    it('should handle single order customer', () => {
      const singleOrder = [mockOrders[0]];
      const result = calculator.calculateCustomerMetrics(mockCustomer, singleOrder, new Date('2024-01-15'));

      assertObjectPartialMatch(result, {
        customer_id: 1,
        total_revenue: 100.00,
        total_orders: 1,
        average_order_value: 100.00,
        customer_lifespan_days: 1, // Minimum 1 day
      });
    });
  });

  describe('calculateTotalRevenue', () => {
    it('should sum order totals correctly', () => {
      const orders = [
        { total_price: 100.50 },
        { total_price: 200.25 },
        { total_price: 50.00 },
      ];

      const result = calculator.calculateTotalRevenue(orders);
      assert.equal(result, 350.75);
    });

    it('should handle missing total_price', () => {
      const orders = [
        { total_price: 100.00 },
        { /* no total_price */ },
        { total_price: 50.00 },
      ];

      const result = calculator.calculateTotalRevenue(orders);
      assert.equal(result, 150.00);
    });
  });

  describe('calculateChurnProbability', () => {
    it('should return low probability for recent customers', () => {
      const metrics = { days_since_last_purchase: 15 };
      const result = calculator.calculateChurnProbability(metrics);
      assert.equal(result, 0.05);
    });

    it('should return high probability for inactive customers', () => {
      const metrics = { days_since_last_purchase: 400 };
      const result = calculator.calculateChurnProbability(metrics);
      assert.equal(result, 0.90);
    });

    it('should return graduated probabilities', () => {
      const testCases = [
        { days: 20, expected: 0.05 },
        { days: 45, expected: 0.15 },
        { days: 75, expected: 0.25 },
        { days: 120, expected: 0.45 },
        { days: 300, expected: 0.70 },
      ];

      testCases.forEach(({ days, expected }) => {
        const metrics = { days_since_last_purchase: days };
        const result = calculator.calculateChurnProbability(metrics);
        assert.equal(result, expected);
      });
    });
  });

  describe('calculateRFMScores', () => {
    const mockCustomer = { id: 1 };
    const baseDate = new Date('2024-03-01');

    it('should calculate high RFM scores for good customer', () => {
      const recentOrders = [
        { processed_at: new Date('2024-02-25'), total_price: 1000 },
        { processed_at: new Date('2024-02-20'), total_price: 1000 },
        { processed_at: new Date('2024-02-15'), total_price: 1000 },
        { processed_at: new Date('2024-02-10'), total_price: 1000 },
        { processed_at: new Date('2024-02-05'), total_price: 1000 },
      ];

      const result = calculator.calculateRFMScores(mockCustomer, recentOrders, baseDate);

      assertObjectPartialMatch(result, {
        recency: 5, // Last order 5 days ago
        frequency: 4, // 5 orders
        monetary: 4, // 5000 total spend
      });

      assert.equal(result.combinedScore, 13);
    });

    it('should calculate low RFM scores for poor customer', () => {
      const oldOrders = [
        { processed_at: new Date('2023-01-01'), total_price: 50 },
      ];

      const result = calculator.calculateRFMScores(mockCustomer, oldOrders, baseDate);

      assertObjectPartialMatch(result, {
        recency: 1, // Very old order
        frequency: 1, // Only 1 order
        monetary: 1, // Low spend
      });

      assert.equal(result.combinedScore, 3);
    });

    it('should handle medium performance customer', () => {
      const mediumOrders = [
        { processed_at: new Date('2024-01-15'), total_price: 300 },
        { processed_at: new Date('2024-01-01'), total_price: 300 },
        { processed_at: new Date('2023-12-15'), total_price: 300 },
      ];

      const result = calculator.calculateRFMScores(mockCustomer, mediumOrders, baseDate);

      assertObjectPartialMatch(result, {
        recency: 2, // ~45 days ago
        frequency: 3, // 3 orders
        monetary: 3, // 900 total spend
      });
    });
  });

  describe('determineSegment', () => {
    it('should identify Champions', () => {
      const rfmScores = { recency: 5, frequency: 5, monetary: 5, combinedScore: 15 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Champions');
    });

    it('should identify Loyal Customers', () => {
      const rfmScores = { recency: 3, frequency: 4, monetary: 4, combinedScore: 11 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Loyal Customers');
    });

    it('should identify New Customers', () => {
      const rfmScores = { recency: 5, frequency: 1, monetary: 2, combinedScore: 8 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'New Customers');
    });

    it('should identify At Risk customers', () => {
      const rfmScores = { recency: 1, frequency: 4, monetary: 4, combinedScore: 9 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'At Risk');
    });

    it('should identify Cannot Lose customers', () => {
      const rfmScores = { recency: 1, frequency: 3, monetary: 5, combinedScore: 9 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Cannot Lose');
    });

    it('should identify Hibernating customers', () => {
      const rfmScores = { recency: 1, frequency: 1, monetary: 1, combinedScore: 3 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Hibernating');
    });

    it('should identify Price Sensitive customers', () => {
      const rfmScores = { recency: 3, frequency: 4, monetary: 1, combinedScore: 8 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Price Sensitive');
    });

    it('should default to Regular for unmatched patterns', () => {
      const rfmScores = { recency: 3, frequency: 3, monetary: 3, combinedScore: 9 };
      const result = calculator.determineSegment(rfmScores);
      assert.equal(result, 'Regular');
    });
  });

  describe('calculateDailyMetrics', () => {
    const mockProducts = [
      { id: 1, title: 'Product A' },
      { id: 2, title: 'Product B' },
    ];

    const mockOrders = [
      {
        id: 1,
        customer_id: 1,
        total_price: 100.00,
        processed_at: new Date('2024-01-15T10:00:00Z'),
        source_name: 'web',
        line_items: [
          {
            source_product_id: '1',
            title: 'Product A',
            quantity: 2,
            price: 50.00,
          },
        ],
      },
      {
        id: 2,
        customer_id: 2,
        total_price: 150.00,
        processed_at: new Date('2024-01-15T14:00:00Z'),
        source_name: 'mobile',
        line_items: [
          {
            source_product_id: '2',
            title: 'Product B',
            quantity: 1,
            price: 150.00,
          },
        ],
      },
      {
        id: 3,
        customer_id: 1, // Same customer, returning
        total_price: 75.00,
        processed_at: new Date('2024-01-15T16:00:00Z'),
        source_name: 'web',
        line_items: [
          {
            source_product_id: '1',
            title: 'Product A',
            quantity: 1,
            price: 75.00,
          },
        ],
      },
    ];

    it('should calculate daily metrics correctly', () => {
      const calculationDate = new Date('2024-01-15');
      const result = calculator.calculateDailyMetrics(mockOrders, mockProducts, calculationDate);

      assertObjectPartialMatch(result, {
        total_revenue: 325.00,
        total_orders: 3,
        total_customers: 2,
        average_order_value: 108.33, // 325 / 3
        total_products_sold: 4, // 2 + 1 + 1
      });

      // Revenue by source
      assert.equal(result.revenue_by_source.web, 175.00);
      assert.equal(result.revenue_by_source.mobile, 150.00);

      // Top selling products
      assert.equal(result.top_selling_products.length, 2);
      assert.equal(result.top_selling_products[0].product_id, '2'); // Higher revenue
      assert.equal(result.top_selling_products[1].product_id, '1');
    });

    it('should handle orders from different dates', () => {
      const ordersWithDifferentDates = [
        {
          ...mockOrders[0],
          processed_at: new Date('2024-01-14T10:00:00Z'), // Different day
        },
        ...mockOrders.slice(1),
      ];

      const calculationDate = new Date('2024-01-15');
      const result = calculator.calculateDailyMetrics(
        ordersWithDifferentDates,
        mockProducts,
        calculationDate
      );

      // Should only include orders from the calculation date
      assert.equal(result.total_orders, 2);
      assert.equal(result.total_revenue, 225.00);
    });

    it('should handle empty orders', () => {
      const result = calculator.calculateDailyMetrics([], mockProducts, new Date());

      assertObjectPartialMatch(result, {
        total_revenue: 0,
        total_orders: 0,
        total_customers: 0,
        average_order_value: 0,
        total_products_sold: 0,
      });

      assert.deepEqual(result.top_selling_products, []);
      assert.deepEqual(result.revenue_by_source, {});
    });
  });

  describe('analyzeCohorts', () => {
    const mockCustomers = [
      {
        id: 1,
        first_purchase_date: new Date('2023-01-15'),
      },
      {
        id: 2,
        first_purchase_date: new Date('2023-01-20'),
      },
      {
        id: 3,
        first_purchase_date: new Date('2023-02-10'),
      },
    ];

    const mockOrders = [
      { id: 1, customer_id: 1, total_price: 100 },
      { id: 2, customer_id: 1, total_price: 150 },
      { id: 3, customer_id: 2, total_price: 200 },
      { id: 4, customer_id: 3, total_price: 75 },
    ];

    it('should analyze cohorts correctly', () => {
      const result = calculator.analyzeCohorts(mockCustomers, mockOrders);

      const jan2023Cohort = result['2023-01'];
      const feb2023Cohort = result['2023-02'];

      assert.ok(jan2023Cohort);
      assert.ok(feb2023Cohort);

      assert.equal(jan2023Cohort.customer_count, 2);
      assert.equal(jan2023Cohort.total_revenue, 450); // 250 + 200
      assert.equal(jan2023Cohort.average_ltv, 225);

      assert.equal(feb2023Cohort.customer_count, 1);
      assert.equal(feb2023Cohort.total_revenue, 75);
      assert.equal(feb2023Cohort.average_ltv, 75);
    });

    it('should handle customers without first purchase date', () => {
      const customersWithoutDate = [
        { id: 1 }, // No first_purchase_date
        { id: 2, first_purchase_date: new Date('2023-01-15') },
      ];

      const result = calculator.analyzeCohorts(customersWithoutDate, mockOrders);

      // Should only process customer with valid date
      assert.equal(Object.keys(result).length, 1);
      assert.ok(result['2023-01']);
    });
  });

  describe('edge cases', () => {
    it('should handle customer with null orders gracefully', () => {
      const customer = { id: 1 };
      const result = calculator.calculateCustomerMetrics(customer, null);

      assert.equal(result.customer_segment, 'Inactive');
      assert.equal(result.churn_probability, 1);
    });

    it('should handle orders with zero total_price', () => {
      const customer = { id: 1 };
      const orders = [
        { id: 1, customer_id: 1, total_price: 0, processed_at: new Date() },
      ];

      const result = calculator.calculateCustomerMetrics(customer, orders);

      assert.equal(result.total_revenue, 0);
      assert.equal(result.average_order_value, 0);
    });

    it('should handle future calculation dates', () => {
      const customer = { id: 1 };
      const orders = [
        { id: 1, customer_id: 1, total_price: 100, processed_at: new Date('2024-01-01') },
      ];
      const futureDate = new Date('2025-01-01');

      const result = calculator.calculateCustomerMetrics(customer, orders, futureDate);

      assert.ok(result.days_since_last_purchase > 300);
      assert.ok(result.churn_probability > 0.5);
    });
  });
});