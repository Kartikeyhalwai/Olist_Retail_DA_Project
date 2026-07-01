# Olist_Retail_DA_Project
Designed and implemented a modern retail analytics workflow covering data ingestion, transformation, KPI development, and dashboard reporting to support data-driven decision-making for e-commerce operations.

# Olist Retail Data Analytics Project

End-to-end data analytics project on the Olist Brazilian e-commerce dataset, built using Databricks, SQL and Power BI. The idea was to take raw multi-table transactional data and turn it into something a business could actually use — clean data, a proper data model, and a dashboard that answers real business questions.

## What this project does

- Cleaned and profiled 9 raw source files (customers, orders, order items, payments, reviews, products, sellers, geolocation, category translations)
- Built a star schema (fact + dimension tables) in Databricks using Spark SQL
- Wrote SQL to answer business questions around revenue, customers, products and sellers
- Built a 5-page Power BI dashboard on top of it

## Tech Stack

- **Databricks** (Unity Catalog, PySpark, Delta Lake)
- **SQL** (Spark SQL)
- **Power BI**

## Architecture

Followed a basic medallion setup:

**Bronze** – raw CSV files loaded into a Unity Catalog Volume, untouched.

**Silver** – each file profiled (shape, nulls, duplicates, datatypes) and cleaned in pandas, then written as Delta tables. Handled things like converting timestamp columns, checking nulls against order status before deciding to drop/keep rows, and filling missing product categories with "Unknown".

**Gold** – built a star schema (`dim_customer`, `dim_product`, `dim_seller`, `dim_date`, `fact_sales`) and a set of analysis tables/marts on top of it for churn, CLV, product demand, seasonality, seller performance, etc.

## Data Model

`fact_sales` is at order-item grain, joined to:
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`

Built with plain `CREATE TABLE AS SELECT` statements in Spark SQL, joining the cleaned Silver tables together.

## SQL Analysis

Wrote queries/marts to answer things like:
- Monthly revenue trend & MoM growth
- Revenue by state, category, payment type
- New vs repeat customers
- Customer churn (Active / At Risk / Churned based on recency)
- Customer lifetime value (Low / Medium / High)
- Top products & categories per state
- Product demand, seasonality, profitability
- Seller performance and review-based rating (Excellent/Good/Average/Poor)
- Delivery time vs review score

## Power BI Dashboard

5 pages:
1. **Executive Overview** – revenue, orders, customers, AOV, payment type, order status
2. **Customer Analysis** – churn rate, repeat vs new customers, CLV segments
3. **Revenue Analysis** – revenue by state/category, monthly & yearly trends
4. **Product Analysis** – revenue/profit by category, top & bottom performing products
5. **Seller Analysis** – top sellers, seller ratings, delivery performance

Some quick numbers from the dashboard: ~20.4M total revenue, 118K orders, ~95K customers, AOV around 207, average review score 4.03.

## Insights

### Retention is the real problem, not acquisition
~97% of customers only ever place one order, and the churn analysis shows around 81% of the customer base is already churned. Revenue is basically being carried entirely by new customers coming in, not by people coming back. If this were a real business, I'd push way harder on retention/loyalty than on getting new customers in the door.

### São Paulo is doing most of the heavy lifting
SP alone brings in around 7.6M of the 20.42M total revenue — more than the next several states combined. It's also home to almost 60% of all sellers on the platform. Revenue and seller supply are basically concentrated in the same place, which makes sense but also means the business is fairly exposed if anything goes wrong in that one state.

### Growth really picked up between 2016 and 2018
Revenue went from almost nothing in 2016 (~0.1M) to about 9.2M in 2017, and then 11.1M in 2018. So more than half of all revenue in the dataset came in the final year alone — this reads like a platform in its early growth phase, not a mature, steady-state business.

### Credit card is the default payment method
Roughly 3 out of 4 transactions (and a similar share of revenue) go through credit card, with boleto a distant second and debit/voucher barely showing up. Not surprising for Brazil, but it does mean payment/checkout experience for credit card users matters way more than optimizing for other payment types.

### A handful of categories drive most of the revenue
Categories like bed/bath/table, beauty, and electronics/accessories sit at the top and pull in noticeably more than the rest, while a long tail of categories (fashion, CDs/DVDs, insurance/services, flowers, etc.) barely register — some show close to 0M in revenue. Worth asking whether some of those low performers are even worth keeping in the catalog.

### Faster delivery genuinely correlates with happier customers
I bucketed orders into delivery windows (1–7 days, 8–14, 15–21, 22+) and checked average review scores against each bucket. Orders delivered faster consistently score higher on reviews. Not a huge surprise, but it was good to actually confirm it in the data instead of assuming it.

### Margins look decent, but it's revenue-driven, not efficiency-driven
Total profit works out to about 6.14M against 20.42M revenue, roughly a 30% margin. That's healthy, but since revenue is so concentrated in a few states/categories, the margin is really riding on those specific pockets performing well rather than the business being efficient everywhere.

### The "product churn" numbers aren't as meaningful as they look
I built a days-since-last-order metric per product, but almost every product landed around the same ~700 day mark. That's really just because the dataset has a fixed cutoff date rather than being live data, so this metric doesn't actually tell you much about which products are genuinely losing demand. Flagging this because it's an easy thing to misread if you don't know the dataset is historical.

## Repository Structure

```
Olist_Retail_DA_Project/
├── Notebooks/
│   └── Data Cleaning & Preparation.ipynb
├── SQL/
│   ├── Star Schema Design for Sales and Customer Analysis.dbquery.ipynb
│   ├── Customer Distribution and Monthly Revenue Trends.dbquery.ipynb
│   ├── Customer Insights.sql
│   ├── Product Sales Analysis and Ranking.dbquery.ipynb
│   ├── Revenue Insights and Customer Analysis.dbquery.ipynb
│   └── Seller Review Category Analysis.dbquery.ipynb
├── olist_retail_powerbi_dashboard.pdf
└── README.md
```

## What I learned

This was my first time really thinking through data modeling decisions instead of just writing queries — like deciding to build the fact table at order-item level instead of order level, and figuring out when a null value is expected vs an actual data issue. Also got a lot more comfortable with window functions (`ROW_NUMBER`, `LAG`) for things like per-state rankings and month-over-month growth.

## Future Improvements

- Incremental loading instead of full overwrite
- RFM-based customer segmentation
- Delivery delay prediction
- Real-time ingestion using Auto Loader
