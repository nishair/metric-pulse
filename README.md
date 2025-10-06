# E-commerce Analytics Pipeline

A comprehensive data pipeline that extracts sales data from multiple e-commerce platforms (Shopify, WooCommerce, Commercetools), transforms it, calculates advanced metrics including Customer Lifetime Value (CLV), and loads everything into PostgreSQL for analysis.

## Features

- **Multi-Platform Support**: Extract data from Shopify, WooCommerce, and Commercetools APIs
- **Customer Analytics**:
  - Customer Lifetime Value (CLV) calculation using multiple methods
  - RFM (Recency, Frequency, Monetary) analysis
  - Customer segmentation (Champions, Loyal, At Risk, etc.)
  - Churn probability prediction
- **Sales Metrics**:
  - Daily revenue tracking
  - Average order value
  - Top selling products
  - Revenue by source
- **Automated Scheduling**: Daily runs via cron scheduling
- **Incremental Loading**: Only processes new/updated data since last run
- **Comprehensive Logging**: Detailed ETL logs with error tracking
- **Rate Limiting**: Respects API rate limits for all platforms

## Architecture Overview

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│                  │     │                  │     │                  │
│    Shopify       │     │   WooCommerce    │     │  Commercetools   │
│      API         │     │       API        │     │       API        │
│                  │     │                  │     │                  │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                         │
         └────────────────┬───────┴─────────────────────────┘
                          │
                    ┌─────▼─────┐
                    │           │
                    │ Connector │
                    │   Layer   │
                    │           │
                    └─────┬─────┘
                          │
                    ┌─────▼─────┐
                    │           │
                    │ Transform │
                    │   Layer   │
                    │           │
                    └─────┬─────┘
                          │
                ┌─────────┴──────────┐
                │                    │
          ┌─────▼─────┐        ┌────▼─────┐
          │           │        │          │
          │    CLV    │        │  Daily   │
          │ Calculator│        │ Metrics  │
          │           │        │          │
          └─────┬─────┘        └────┬─────┘
                │                    │
                └─────────┬──────────┘
                          │
                    ┌─────▼─────┐
                    │           │
                    │PostgreSQL │
                    │  Loader   │
                    │           │
                    └─────┬─────┘
                          │
                    ┌─────▼─────┐
                    │           │
                    │PostgreSQL │
                    │ Database  │
                    │           │
                    └───────────┘
```

## Prerequisites

- Node.js v18 or higher
- PostgreSQL 12 or higher
- API credentials for at least one platform:
  - Shopify: Store URL and Private App Access Token
  - WooCommerce: Site URL, Consumer Key, and Consumer Secret
  - Commercetools: Project Key, Client ID, Client Secret, and Region

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/metric-pulse.git
cd metric-pulse
```

2. Install dependencies:
```bash
npm install
```

3. Copy the environment configuration:
```bash
cp .env.example .env
```

4. Configure your `.env` file with your API credentials and database connection:
```env
# Shopify API Configuration
SHOPIFY_STORE_URL=https://your-store.myshopify.com
SHOPIFY_ACCESS_TOKEN=your-shopify-access-token

# WooCommerce API Configuration
WOOCOMMERCE_URL=https://your-woocommerce-site.com
WOOCOMMERCE_CONSUMER_KEY=your-consumer-key
WOOCOMMERCE_CONSUMER_SECRET=your-consumer-secret

# Commercetools API Configuration
COMMERCETOOLS_PROJECT_KEY=your-project-key
COMMERCETOOLS_CLIENT_ID=your-client-id
COMMERCETOOLS_CLIENT_SECRET=your-client-secret
COMMERCETOOLS_REGION=us-central1

# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_analytics
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# Pipeline Configuration
ENABLE_SHOPIFY=true
ENABLE_WOOCOMMERCE=false
ENABLE_COMMERCETOOLS=false
SCHEDULE_CRON=0 2 * * *
```

5. Create the database and run migrations:
```bash
createdb ecommerce_analytics
npm run migrate
```

## Usage

### Run Pipeline Immediately
```bash
npm start -- --run-now
```

### Start Scheduled Daily Runs
```bash
npm start -- --schedule
```

The default schedule is 2 AM daily, but you can customize it using the `SCHEDULE_CRON` environment variable.

### View Available Commands
```bash
npm start
```

## Database Schema

The pipeline creates the following main tables:

- **customers**: Customer profiles with contact and location data
- **products**: Product catalog with pricing and inventory
- **orders**: Order transactions with financial details
- **order_items**: Individual line items within orders
- **customer_metrics**: CLV calculations and RFM scores per customer
- **daily_metrics**: Aggregated daily business metrics
- **etl_logs**: Pipeline run history and error tracking

## Customer Lifetime Value Calculation

The pipeline calculates CLV using multiple methods:

1. **Simple CLV**: Average Order Value × Purchase Frequency × Customer Lifespan
2. **Predictive CLV**: Incorporates retention rate and discount rate for future value prediction
3. **RFM Analysis**: Scores customers on:
   - Recency: Days since last purchase
   - Frequency: Number of orders
   - Monetary: Total spend

## Customer Segmentation

Based on RFM scores, customers are automatically segmented into:

- **Champions**: High value, frequent, recent buyers
- **Loyal Customers**: Good frequency and monetary value
- **Potential Loyalists**: Recent customers with growth potential
- **New Customers**: Recent first-time buyers
- **At Risk**: Previously good customers showing decline
- **Cannot Lose**: High value customers becoming inactive
- **Hibernating**: Low activity across all dimensions
- **Price Sensitive**: Frequent buyers with low monetary value

## API Rate Limiting

The pipeline implements intelligent rate limiting:

- **Shopify**: 2 calls/second with backoff at 80% limit
- **WooCommerce**: 3 calls/second
- **Commercetools**: 5 calls/second with OAuth token management

## Monitoring and Logging

Logs are stored in the `logs/` directory:

- `combined.log`: All pipeline activity
- `error.log`: Errors and warnings only

ETL runs are also tracked in the `etl_logs` database table with:
- Pipeline execution times
- Record counts (extracted/transformed/loaded)
- Error messages
- Success/failure status

## Development

### Project Structure
```
metric-pulse/
├── src/
│   ├── connectors/       # API connectors for each platform
│   ├── transformers/     # Data transformation logic
│   ├── analytics/        # CLV and metric calculations
│   ├── db/              # Database operations and migrations
│   ├── pipelines/       # ETL orchestration
│   ├── utils/           # Configuration and logging
│   └── index.js         # Main entry point
├── logs/                # Application logs
├── package.json         # Dependencies
├── .env.example         # Environment configuration template
└── README.md           # This file
```

### Adding a New E-commerce Platform

1. Create a new connector in `src/connectors/`
2. Implement the required methods:
   - `testConnection()`
   - `getCustomers()`
   - `getOrders()`
   - `getProducts()`
3. Add configuration to `src/utils/config.js`
4. Register the connector in `src/pipelines/etlPipeline.js`

### Testing

Run a test pipeline execution:
```bash
NODE_ENV=test npm start -- --run-now
```

## Performance Considerations

- **Batch Processing**: Data is processed in configurable batch sizes
- **Incremental Loading**: Only new/modified records since last run
- **Connection Pooling**: PostgreSQL connections are pooled for efficiency
- **Pagination**: All API calls use pagination to handle large datasets

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify PostgreSQL is running
   - Check database credentials in `.env`
   - Ensure database exists: `createdb ecommerce_analytics`

2. **API Authentication Errors**
   - Verify API credentials are correct
   - Check API permissions (read access to customers, orders, products)
   - For Shopify: Ensure private app has required scopes

3. **Rate Limiting**
   - The pipeline automatically handles rate limits
   - If issues persist, increase `rateLimitDelay` in connectors

4. **Memory Issues with Large Datasets**
   - Reduce `BATCH_SIZE` in environment variables
   - Increase Node.js memory: `node --max-old-space-size=4096`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT

## Support

For issues and questions, please create an issue in the GitHub repository.