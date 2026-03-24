# 0Assignment 3: E-commerce dbt Project (DuckDB)

## 📌 Business Context
**Domain:** E-commerce
This analytical platform processes raw e-commerce data to generate business-ready insights. The data entities include users, products, orders, order items, and promotional campaigns.

**Business Questions Answered by the Marts Layer:**
- Who are our VIP customers based on their Lifetime Value (LTV)?
- What is our daily and monthly revenue?
- What is the profit margin for our orders considering product costs?
- What is the time interval between repeat purchases for our customers?

---

## ✅ Technical Requirements Checklist

This project successfully fulfills all the assignment requirements:

- [x] **1. Use DuckDB:** The project runs entirely locally using the `dbt-duckdb` adapter.
- [x] **2. Source Data in Seeds:** 5 base datasets are stored as CSVs in the `seeds/` folder (`users`, `products`, `orders`, `order_items`, `promotions`).
- [x] **3. Minimum 20 dbt Models:** Exactly 20 models were built, strictly following dimensional modeling best practices (Layers: Staging -> Dimensions/Facts -> Marts) to maintain correct data grain and prevent fan-out.
- [x] **4. At Least 5 Incremental Models:** Implemented `incremental` materialization with `unique_key` configurations in:
  1. `fct_orders`
  2. `fct_order_items`
  3. `mart_daily_revenue`
  4. `mart_monthly_revenue`
  5. `mart_returned_orders`
- [x] **5. Incremental Predicate Logic:** Applied in `fct_orders.sql` (`DBT_INTERNAL_DEST.order_date >= current_date - interval 30 day`). This optimizes the `MERGE` operation by only scanning the last 30 days of historical data instead of the entire table.
- [x] **6. Custom Macro (Bonus +2.5):** Created `calculate_margin(price, cost)` in the `macros/` directory. It is used in `mart_marginal_analysis.sql` to calculate profit, adhering to the DRY principle.
- [x] **7. Use Window Functions:** - `LAG()`: Used in `mart_order_intervals` to partition by user and find the date of their previous order.
  - `AVG() OVER()`: Used in `mart_product_performance` to analyze revenue distribution.
- [x] **8. Add Tests:**
  - **Generic Tests:** Configured in `models/staging/schema.yml` (`unique`, `not_null`, `accepted_values` for statuses, and `relationships` for foreign keys).
  - **Singular Test:** Created `tests/assert_updated_at_after_order_date.sql` to ensure data quality by catching records where the update timestamp illogically precedes the creation timestamp.
- [x] **9. Style Guide & Architecture:** Consistent naming conventions used (`stg_`, `dim_`, `fct_`, `mart_`). All `mart` models correctly reference `dim`/`fct` models, NOT raw staging tables.
- [x] **10. Provide Data Insights:** Analytical marts like `mart_user_lifetime_value` and `mart_sales_by_city` are ready for BI tool consumption.

---

## 🧠 Theoretical Questions

**1. What is the purpose of dbt in a modern data stack?**
dbt (data build tool) handles the "T" (Transformation) in the ELT process. It allows engineers to transform data already loaded in the warehouse using modular SQL, while applying software engineering best practices like version control, data testing, and automated documentation.

**2. What is the difference between a seed, a source, and a model in dbt?**
* **Seed:** A local CSV file that dbt explicitly loads into the database as a table.
* **Source:** A declaration of an existing, raw table already present in the database that dbt can reference.
* **Model:** A `.sql` file containing a `SELECT` statement that transforms data.

**3. What is the difference between table, view, and incremental materializations?**
* **View:** A virtual table; the SQL query runs every time the view is queried.
* **Table:** A physical table; dbt drops the old table and rebuilds it entirely from scratch on every run.
* **Incremental:** A physical table that only inserts or updates records that have changed or been added since the last run, significantly saving compute time and resources.

**4. What is the purpose of the staging layer in a dbt project?**
The staging layer acts as the foundation. Its sole purpose is to clean raw data, standardize column names (aliasing), and cast correct data types. It creates a consistent "data contract" before joining tables in downstream models.

**5. What is the difference between a dimension model and a fact model?**
* **Dimension:** A table containing descriptive attributes (who, what, where) like customers or products. They are usually wide and update slowly.
* **Fact:** A table recording specific measurable events or transactions (how much, when) like orders or clicks. They are usually narrow, contain foreign keys to dimensions, and grow rapidly.

**6. Why are tests important in dbt, and what is the difference between generic and singular tests?**
Tests ensure data quality and integrity, preventing bad data from reaching business dashboards. 
* **Generic tests:** Built-in, reusable tests configured in YAML files (e.g., `not_null`, `unique`).
* **Singular tests:** Custom `.sql` queries written to check specific business logic; the test fails if the query returns any rows.

**7. What is a macro in dbt, and when should you create one?**
A macro is a reusable piece of Jinja-templated SQL code. You should create one when you find yourself repeating the same SQL logic (like a specific calculation) across multiple models, adhering to the DRY (Don't Repeat Yourself) principle.

**8. What is an incremental predicate, and how does it improve model performance?**
An incremental predicate is an additional filter (e.g., scanning only the last 30 days) applied during the `merge` process of an incremental run. It instructs the database engine to only scan recent partitions of the target table rather than the entire history, drastically improving query speed.

**9. Why are window functions useful in analytics engineering?**
Window functions allow you to perform calculations across a set of related rows (like running totals, rankings, or finding previous values using `LAG()`) *without* collapsing the rows, which happens when using standard `GROUP BY` aggregations.

**10. What does it mean to describe the grain of a model, and why is grain important?**
The grain defines exactly what one single row in a table represents (e.g., "one row = one order" vs. "one row = one item inside an order"). Defining the grain is critical to prevent data duplication ("fan-out") and inaccurate metrics when joining tables.
