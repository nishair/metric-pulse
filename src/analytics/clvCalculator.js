import { differenceInDays, subDays } from 'date-fns';
import { logger } from '../utils/logger.js';

export class CLVCalculator {
  constructor() {
    this.defaultChurnDays = 365; // Consider customer churned after 1 year of inactivity
    this.rfmWeights = {
      recency: 0.35,
      frequency: 0.35,
      monetary: 0.30,
    };
  }

  calculateCustomerMetrics(customer, orders, calculationDate = new Date()) {
    try {
      const customerOrders = orders.filter(o => o.customer_id === customer.id);

      if (customerOrders.length === 0) {
        return this.getEmptyMetrics(customer.id, calculationDate);
      }

      // Sort orders by date
      customerOrders.sort((a, b) => new Date(a.processed_at) - new Date(b.processed_at));

      const metrics = {
        customer_id: customer.id,
        calculation_date: calculationDate,
        total_revenue: this.calculateTotalRevenue(customerOrders),
        total_orders: customerOrders.length,
        average_order_value: 0,
        purchase_frequency: 0,
        customer_lifespan_days: 0,
        customer_lifetime_value: 0,
        churn_probability: 0,
        days_since_last_purchase: 0,
        rfm_recency_score: 0,
        rfm_frequency_score: 0,
        rfm_monetary_score: 0,
        customer_segment: '',
      };

      // Calculate basic metrics
      metrics.average_order_value = metrics.total_revenue / metrics.total_orders;

      // Calculate customer lifespan
      const firstOrderDate = new Date(customerOrders[0].processed_at);
      const lastOrderDate = new Date(customerOrders[customerOrders.length - 1].processed_at);
      metrics.customer_lifespan_days = differenceInDays(lastOrderDate, firstOrderDate) || 1;
      metrics.days_since_last_purchase = differenceInDays(calculationDate, lastOrderDate);

      // Calculate purchase frequency (orders per month)
      const monthsActive = Math.max(metrics.customer_lifespan_days / 30, 1);
      metrics.purchase_frequency = metrics.total_orders / monthsActive;

      // Calculate CLV using different methods
      const simpleCLV = this.calculateSimpleCLV(metrics);
      const predictiveCLV = this.calculatePredictiveCLV(metrics);
      metrics.customer_lifetime_value = (simpleCLV + predictiveCLV) / 2;

      // Calculate churn probability
      metrics.churn_probability = this.calculateChurnProbability(metrics);

      // Calculate RFM scores
      const rfmScores = this.calculateRFMScores(customer, customerOrders, calculationDate);
      metrics.rfm_recency_score = rfmScores.recency;
      metrics.rfm_frequency_score = rfmScores.frequency;
      metrics.rfm_monetary_score = rfmScores.monetary;

      // Determine customer segment
      metrics.customer_segment = this.determineSegment(rfmScores);

      return metrics;
    } catch (error) {
      logger.error('Error calculating customer metrics', { error, customerId: customer.id });
      throw error;
    }
  }

  calculateTotalRevenue(orders) {
    return orders.reduce((sum, order) => sum + (order.total_price || 0), 0);
  }

  calculateSimpleCLV(metrics) {
    // Simple CLV = Average Order Value × Purchase Frequency × Customer Lifespan (in months)
    const lifespanMonths = metrics.customer_lifespan_days / 30;
    return metrics.average_order_value * metrics.purchase_frequency * lifespanMonths;
  }

  calculatePredictiveCLV(metrics) {
    // Predictive CLV with churn consideration
    const monthlyRevenue = metrics.average_order_value * metrics.purchase_frequency;
    const retentionRate = 1 - metrics.churn_probability;
    const discountRate = 0.1 / 12; // 10% annual discount rate, monthly

    // CLV = Monthly Revenue × (Retention Rate / (1 + Discount Rate - Retention Rate))
    if (retentionRate >= (1 + discountRate)) {
      // If retention is too high, use a simpler calculation
      return monthlyRevenue * 12 * 3; // 3 year projection
    }

    return monthlyRevenue * (retentionRate / (1 + discountRate - retentionRate));
  }

  calculateChurnProbability(metrics) {
    // Simple churn probability based on days since last purchase
    const daysSinceLastPurchase = metrics.days_since_last_purchase;

    if (daysSinceLastPurchase < 30) return 0.05;
    if (daysSinceLastPurchase < 60) return 0.15;
    if (daysSinceLastPurchase < 90) return 0.25;
    if (daysSinceLastPurchase < 180) return 0.45;
    if (daysSinceLastPurchase < 365) return 0.70;
    return 0.90;
  }

  calculateRFMScores(customer, orders, calculationDate) {
    // Recency Score
    const lastOrderDate = new Date(orders[orders.length - 1].processed_at);
    const daysSinceLastOrder = differenceInDays(calculationDate, lastOrderDate);

    let recencyScore;
    if (daysSinceLastOrder <= 30) recencyScore = 5;
    else if (daysSinceLastOrder <= 60) recencyScore = 4;
    else if (daysSinceLastOrder <= 90) recencyScore = 3;
    else if (daysSinceLastOrder <= 180) recencyScore = 2;
    else recencyScore = 1;

    // Frequency Score
    const orderCount = orders.length;
    let frequencyScore;
    if (orderCount >= 20) frequencyScore = 5;
    else if (orderCount >= 10) frequencyScore = 4;
    else if (orderCount >= 5) frequencyScore = 3;
    else if (orderCount >= 2) frequencyScore = 2;
    else frequencyScore = 1;

    // Monetary Score
    const totalSpent = this.calculateTotalRevenue(orders);
    let monetaryScore;
    if (totalSpent >= 5000) monetaryScore = 5;
    else if (totalSpent >= 2000) monetaryScore = 4;
    else if (totalSpent >= 500) monetaryScore = 3;
    else if (totalSpent >= 100) monetaryScore = 2;
    else monetaryScore = 1;

    return {
      recency: recencyScore,
      frequency: frequencyScore,
      monetary: monetaryScore,
      combinedScore: recencyScore + frequencyScore + monetaryScore,
    };
  }

  determineSegment(rfmScores) {
    const { recency, frequency, monetary, combinedScore } = rfmScores;

    // Champions
    if (recency >= 4 && frequency >= 4 && monetary >= 4) {
      return 'Champions';
    }

    // Loyal Customers
    if (frequency >= 3 && monetary >= 3 && combinedScore >= 9) {
      return 'Loyal Customers';
    }

    // Potential Loyalists
    if (recency >= 3 && frequency >= 2 && combinedScore >= 7) {
      return 'Potential Loyalists';
    }

    // Recent Customers
    if (recency >= 4 && frequency <= 2) {
      return 'New Customers';
    }

    // At Risk
    if (recency <= 2 && frequency >= 3 && monetary >= 3) {
      return 'At Risk';
    }

    // Can't Lose Them
    if (recency <= 2 && monetary >= 4) {
      return 'Cannot Lose';
    }

    // Hibernating
    if (recency <= 2 && frequency <= 2 && monetary <= 2) {
      return 'Hibernating';
    }

    // Price Sensitive
    if (monetary <= 2 && frequency >= 3) {
      return 'Price Sensitive';
    }

    return 'Regular';
  }

  getEmptyMetrics(customerId, calculationDate) {
    return {
      customer_id: customerId,
      calculation_date: calculationDate,
      total_revenue: 0,
      total_orders: 0,
      average_order_value: 0,
      purchase_frequency: 0,
      customer_lifespan_days: 0,
      customer_lifetime_value: 0,
      churn_probability: 1,
      days_since_last_purchase: null,
      rfm_recency_score: 1,
      rfm_frequency_score: 1,
      rfm_monetary_score: 1,
      customer_segment: 'Inactive',
    };
  }

  calculateDailyMetrics(orders, products, calculationDate = new Date()) {
    const dayOrders = orders.filter(order => {
      const orderDate = new Date(order.processed_at);
      return orderDate.toDateString() === calculationDate.toDateString();
    });

    const metrics = {
      metric_date: calculationDate,
      total_revenue: 0,
      total_orders: dayOrders.length,
      total_customers: new Set(),
      new_customers: 0,
      returning_customers: 0,
      average_order_value: 0,
      total_products_sold: 0,
      top_selling_products: [],
      revenue_by_source: {},
    };

    // Calculate metrics
    const productSales = {};
    const customerOrderCounts = {};

    dayOrders.forEach(order => {
      metrics.total_revenue += order.total_price || 0;

      if (order.customer_id) {
        metrics.total_customers.add(order.customer_id);
        customerOrderCounts[order.customer_id] = (customerOrderCounts[order.customer_id] || 0) + 1;
      }

      // Track revenue by source
      const source = order.source_name || 'direct';
      metrics.revenue_by_source[source] = (metrics.revenue_by_source[source] || 0) + (order.total_price || 0);

      // Count products sold
      if (order.line_items) {
        order.line_items.forEach(item => {
          metrics.total_products_sold += item.quantity || 0;

          const productId = item.source_product_id;
          if (productId) {
            if (!productSales[productId]) {
              productSales[productId] = {
                product_id: productId,
                title: item.title,
                quantity: 0,
                revenue: 0,
              };
            }
            productSales[productId].quantity += item.quantity || 0;
            productSales[productId].revenue += (item.price || 0) * (item.quantity || 0);
          }
        });
      }
    });

    // Calculate average order value
    if (metrics.total_orders > 0) {
      metrics.average_order_value = metrics.total_revenue / metrics.total_orders;
    }

    // Get top selling products
    metrics.top_selling_products = Object.values(productSales)
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 10);

    // Convert customer set to count
    metrics.total_customers = metrics.total_customers.size;

    // Determine new vs returning customers (simplified)
    Object.entries(customerOrderCounts).forEach(([customerId, count]) => {
      if (count === 1) {
        metrics.new_customers++;
      } else {
        metrics.returning_customers++;
      }
    });

    return metrics;
  }

  // Cohort analysis
  analyzeCohorts(customers, orders, cohortMonths = 12) {
    const cohorts = {};
    const now = new Date();

    customers.forEach(customer => {
      const firstOrderDate = customer.first_purchase_date;
      if (!firstOrderDate) return;

      const cohortKey = `${firstOrderDate.getFullYear()}-${String(firstOrderDate.getMonth() + 1).padStart(2, '0')}`;

      if (!cohorts[cohortKey]) {
        cohorts[cohortKey] = {
          cohort_month: cohortKey,
          customer_count: 0,
          total_revenue: 0,
          average_ltv: 0,
          retention_rates: {},
        };
      }

      cohorts[cohortKey].customer_count++;

      // Calculate revenue for this customer
      const customerOrders = orders.filter(o => o.customer_id === customer.id);
      const customerRevenue = this.calculateTotalRevenue(customerOrders);
      cohorts[cohortKey].total_revenue += customerRevenue;
    });

    // Calculate average LTV per cohort
    Object.values(cohorts).forEach(cohort => {
      if (cohort.customer_count > 0) {
        cohort.average_ltv = cohort.total_revenue / cohort.customer_count;
      }
    });

    return cohorts;
  }
}