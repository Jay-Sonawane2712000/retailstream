# RetailStream Data Dictionary

## 1. Source Dataset
RetailStream uses the **Olist eCommerce dataset**, a public marketplace dataset that contains retail order activity, customer information, product metadata, seller data, payments, and reviews.

### CSV Files Used
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`

## 2. Bronze Tables
The Bronze layer stores raw ingested data in DuckDB with minimal transformation.

### `bronze_orders`
- Purpose: Stores order-level records from the source system.
- Primary Key: `order_id`
- Key columns: `order_id`, `customer_id`, `order_status`, purchase and delivery timestamps.
- Join keys: `order_id`, `customer_id`.

### `bronze_order_items`
- Purpose: Stores order line items, including product, seller, and pricing details.
- Primary Key: (`order_id`, `order_item_id`)
- Key columns: `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value`.
- Join keys: `order_id`, `product_id`, `seller_id`.

### `bronze_customers`
- Purpose: Stores customer reference and location information.
- Primary Key: `customer_id`
- Key columns: `customer_id`, `customer_unique_id`, `customer_city`, `customer_state`.
- Join keys: `customer_id`.

### `bronze_products`
- Purpose: Stores product attributes and physical characteristics.
- Primary Key: `product_id`
- Key columns: `product_id`, `product_category_name`, weight and size columns.
- Join keys: `product_id`.

### `bronze_sellers`
- Purpose: Stores seller reference and location information.
- Primary Key: `seller_id`
- Key columns: `seller_id`, `seller_city`, `seller_state`.
- Join keys: `seller_id`.

### `bronze_payments`
- Purpose: Stores payment method, installments, and payment amount at the order level.
- Primary Key: (`order_id`, `payment_sequential`)
- Key columns: `order_id`, `payment_type`, `payment_value`.
- Join keys: `order_id`.

### `bronze_reviews`
- Purpose: Stores review scores and review timestamps for completed orders.
- Primary Key: `review_id`
- Key columns: `review_id`, `order_id`, `review_score`.
- Join keys: `review_id`, `order_id`.

### `bronze_metadata`
- Purpose: Stores ingestion metadata for Bronze loads.
- Key columns: `table_name`, `row_count`, `load_timestamp`, `source_file`.
- Join keys: not typically used for joins; supports load auditing and monitoring.

## 3. dbt Staging Models
The staging layer cleans and standardizes Bronze data for analytics-ready modeling.

### `stg_orders`
- Cleans order records from `bronze_orders`.
- Primary Key: `order_id`
- Logic includes null filtering on `order_id`, safe timestamp casting, lowercase `order_status`, and deduplication by `order_id` using the latest `order_purchase_timestamp`.

### `stg_order_items`
- Cleans order item records from `bronze_order_items`.
- Primary Key: (`order_id`, `order_item_id`)
- Logic includes required key filtering on `order_id`, `product_id`, and `seller_id`, safe casting for numeric and timestamp fields, and removal of rows where `price <= 0`.

### `stg_customers`
- Cleans customer reference data from `bronze_customers`.
- Primary Key: `customer_id`
- Logic includes required key filtering, safe integer casting for zip code, and uppercase standardization for `customer_city` and `customer_state`.

### `stg_products`
- Cleans product reference data from `bronze_products`.
- Primary Key: `product_id`
- Logic includes required key filtering on `product_id`, safe numeric casting, and defaulting missing `product_category_name` values to `unknown`.

### `stg_sellers`
- Cleans seller reference data from `bronze_sellers`.
- Primary Key: `seller_id`
- Logic includes required key filtering, safe integer casting for zip code, and uppercase standardization for `seller_city` and `seller_state`.

### `stg_payments`
- Cleans payment data from `bronze_payments`.
- Primary Key: (`order_id`, `payment_sequential`)
- Logic includes required key filtering on `order_id`, lowercase standardization for `payment_type`, and safe numeric casting for payment fields.

### `stg_reviews`
- Cleans review data from `bronze_reviews`.
- Primary Key: `review_id`
- Logic includes required key filtering on `review_id` and `order_id`, safe timestamp and integer casting, and deduplication by `review_id` using `ROW_NUMBER()`.

## 4. Gold Dimension and Fact Tables
The Gold layer organizes cleaned staging data into analytics-ready dimensions and facts.

### `dim_customers`
- Customer dimension with one row per `customer_id`.
- Primary Key: `customer_id`
- Includes customer identity and location attributes.

### `dim_products`
- Product dimension with one row per `product_id`.
- Primary Key: `product_id`
- Includes category and physical product attributes.

### `dim_sellers`
- Seller dimension with one row per `seller_id`.
- Primary Key: `seller_id`
- Includes seller location attributes.

### `dim_date`
- Date dimension derived from order purchase dates.
- Primary Key: `date_id`
- Includes calendar fields such as year, quarter, month, week, day of week, and weekend flag.

### `fact_orders`
- Central fact table for retail transaction analysis.
- Grain: **one row per order line item**.
- Primary Key: (`order_id`, `order_item_id`)
- Foreign Keys:
  `customer_id` -> `dim_customers.customer_id`
  `product_id` -> `dim_products.product_id`
  `seller_id` -> `dim_sellers.seller_id`
  `date_id` -> `dim_date.date_id`
- Combines order, item, payment, customer, seller, and review context for downstream KPI analysis.
- Payments are aggregated at the order level before joining to `fact_orders` to avoid revenue duplication at the line-item grain.

## 5. Reporting Mart Tables
The reporting marts provide business-friendly summary tables for dashboards and KPI reporting.

### `mart_sales_daily`
- Daily sales summary by date.
- Includes total orders, total revenue, and average order value.

### `mart_product_performance`
- Product-level performance summary.
- Includes total orders, total revenue, and average selling price by product.

### `mart_customer_retention`
- Customer-level retention summary.
- Includes total orders, first order date, last order date, and repeat customer flag.

### `mart_seller_performance`
- Seller-level performance summary.
- Includes total orders, total revenue, and average delivery days.
