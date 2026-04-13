# From Raw Data to 98% Agent Reliability

A step-by-step guide using dbt and OnlyMetrix.

By the end of this tutorial you will have:

- A PostgreSQL database loaded with the UCI Online Retail dataset
- A dbt project with staging and mart models
- OnlyMetrix compiling your dbt models into a governed metric layer
- An analytics agent answering 20 business questions with 98% reliability and zero fabrications
- A reproducible benchmark you can run on your own data

Time required: about 2 hours. Most of that is waiting for data loads and benchmark runs.

## Prerequisites

- Python 3.10+
- PostgreSQL 14+ (local or Docker)
- dbt Core 1.7+ (`pip install dbt-postgres`)
- OnlyMetrix (`pip install onlymetrix`)
- Anthropic API key (for the agent benchmark)

```bash
pip install onlymetrix dbt-postgres pandas openpyxl psycopg2-binary httpx pyyaml anthropic
```

## Step 1: Load the data

The UCI Online Retail dataset is a real e-commerce transaction log from a UK-based retailer. About 500K line items, 45K invoices, 6K customers, 5K products. It is messy in the ways real data is messy: missing customer IDs, negative quantities for returns, cancellation invoices prefixed with "C".

Create a PostgreSQL database and load the data:

```bash
createdb retail

python scripts/load_data.py \
  --db-url postgres://localhost:5432/retail
```

Output:

```
Step 1: Download dataset
  Downloading from UCI archive...
  Extracting...
Step 2: Connect to database
Step 3: Create schema
Step 4: Load data
  Reading Excel file...
  Loading products...
  Loading customers...
  Loading invoices...
  Loading invoice items (this takes a moment)...
    10000/541909 items...
    ...

Done. Table counts:
  customers: 5,942 rows
  products: 4,646 rows
  invoices: 44,876 rows
  invoice_items: 824,364 rows
```

The load script downloads the Excel file from UCI's archive, normalizes it into four tables with proper foreign keys, and computes derived fields (churn flags, line totals, cancellation flags).

Verify the schema looks right:

```bash
psql retail -c "\dt"
```

```
          List of relations
 Schema |     Name      | Type  | Owner
--------+---------------+-------+-------
 public | customers     | table | you
 public | invoice_items | table | you
 public | invoices      | table | you
 public | products      | table | you
```

## Step 2: Build the dbt project

The `benchmark_dbt/` directory contains a complete dbt project. The structure:

```
benchmark_dbt/
  dbt_project.yml
  profiles.yml
  models/
    staging/
      stg_invoices.sql        # Clean pass-through from raw invoices
      stg_customers.sql       # Clean pass-through from raw customers
      stg_products.sql        # Clean pass-through from raw products
      stg_invoice_items.sql   # Clean pass-through from raw invoice_items
    marts/
      fct_revenue.sql         # Total revenue excluding cancellations
      fct_orders.sql          # Invoice counts
      fct_customers.sql       # Customer count with churn metrics
      fct_avg_order.sql       # Average order value
      fct_products.sql        # Top products by revenue (multi-table join)
      fct_monthly_revenue.sql # Monthly revenue time series
      fct_quarterly_revenue.sql
      fct_revenue_by_country.sql
    schema.yml                # Column descriptions and docs
```

The staging models are pass-throughs. The mart models are the interesting part. Here is what each one computes:

**fct_revenue.sql** — the simplest metric, total revenue excluding cancellations:

```sql
SELECT SUM(total_amount) AS total_revenue
FROM public.invoices
WHERE NOT is_cancellation
```

**fct_products.sql** — a multi-table join across invoices, invoice_items, and products:

```sql
SELECT
    p.description, p.stock_code,
    SUM(ii.quantity) AS units_sold,
    SUM(ii.line_total) AS revenue
FROM public.invoice_items ii
JOIN public.products p ON p.id = ii.product_id
JOIN public.invoices i ON i.id = ii.invoice_id
WHERE NOT i.is_cancellation AND ii.quantity > 0
GROUP BY p.description, p.stock_code
ORDER BY revenue DESC
```

**fct_customers.sql** — aggregates customer count, churned count, and churn percentage in one query:

```sql
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS churn_pct
FROM public.customers
```

The key thing: these are standard dbt SQL models. No MetricFlow YAML. No semantic layer configuration files. OnlyMetrix reads the SQL directly.

Set your database credentials and run:

```bash
export DB_USER=postgres
export DB_PASSWORD=yourpassword
export DB_NAME=retail

cd benchmark_dbt
dbt run
```

```
Running with dbt=1.7.0
Found 12 models, 0 tests, 0 snapshots, 0 analyses

Concurrency: 4 threads (target='dev')

1 of 12 START sql view model public.stg_invoices .............. [RUN]
2 of 12 START sql view model public.stg_customers ............. [RUN]
...
12 of 12 OK created sql view model public.fct_products ........ [OK]

Finished running 12 view models in 0 hours 0 minutes and 2.13 seconds.

Completed successfully
Done. PASS=12 WARN=0 ERROR=0 SKIP=0 TOTAL=12
```

All 12 models should pass. If any fail, check your `profiles.yml` credentials.

## Step 3: Configure OnlyMetrix metrics

The `metrics.yaml` file in the repo root defines 24 metrics that map to the benchmark questions. Each metric has:

- A name the agent can reference
- A description the agent reads to decide which metric to use
- The SQL that executes when the metric is queried
- Source tables for join resolution

Here is what a few of them look like:

```yaml
- name: total_revenue
  description: "Total revenue in GBP excluding cancellations.
    Use for 'what is total revenue' or 'how much revenue'."
  sql: "SELECT SUM(total_amount) AS revenue_gbp
    FROM invoices WHERE NOT is_cancellation"
  source_tables: [invoices]

- name: cancellation_rate
  description: "Percentage of orders that are cancellations.
    Use for 'cancellation rate', 'what percentage are cancellations'."
  sql: |
    SELECT ROUND(
      SUM(CASE WHEN is_cancellation THEN 1 ELSE 0 END) * 100.0
      / COUNT(*), 1
    ) AS cancel_pct FROM invoices
  source_tables: [invoices]

- name: top_products
  description: "Top products by revenue with units sold.
    Already grouped by product -- do NOT add a dimension parameter."
  sql: |
    SELECT p.description, p.stock_code,
           SUM(ii.quantity) AS units_sold,
           SUM(ii.line_total) AS revenue_gbp
    FROM invoice_items ii
    JOIN products p ON p.id = ii.product_id
    JOIN invoices i ON i.id = ii.invoice_id
    WHERE NOT i.is_cancellation AND ii.quantity > 0
    GROUP BY p.description, p.stock_code
    ORDER BY revenue_gbp DESC
  source_tables: [invoice_items, products, invoices]
```

Notice the description on `top_products`: "Already grouped by product -- do NOT add a dimension parameter." This matters. The agent reads these descriptions to decide how to call the metric. If the description does not say the metric is pre-grouped, the agent will try to add a dimension, which hits a different code path in the backend. We learned this the hard way (see Step 7).

Start the OnlyMetrix server:

```bash
export DATAQUERY_CONFIG=metrics.yaml
# If running from source:
./target/release/dataquery-mcp
# Or via pip:
omx serve --config metrics.yaml
```

Verify metrics are loaded:

```bash
python scripts/verify_setup.py --api-url http://localhost:3001
```

```
Checking OnlyMetrix API...
  OK: 24 metrics loaded
  OK: total_revenue = 17743429.16

Setup verified. Ready to run benchmark.
```

## Step 4: Run the SQL baseline

Before we run the OM agent, let us establish a baseline. The SQL strategy gives Claude Sonnet the full database schema and asks it to generate SQL from scratch for each question. This is the standard text-to-SQL approach.

```bash
export ANTHROPIC_API_KEY=sk-ant-...

python run_bench.py \
  --strategies sql \
  --model claude-sonnet-4-6 \
  --iterations 3 \
  --api-url http://localhost:3001
```

This takes 5-10 minutes (60 LLM calls). Output:

```
Strategy: sql (claude-sonnet-4-6)
============================================================
  v total_revenue                    100% (3/3)
  v total_customers                  100% (3/3)
  v total_invoices                   100% (3/3)
  X avg_order_value                    0% (0/3)
  v revenue_by_country_top5          100% (3/3)
  X monthly_revenue                    0% (0/3)
  v churn_count                      100% (3/3)
  v churn_rate                       100% (3/3)
  X top_products_by_revenue            0% (0/3)
  X revenue_by_quarter                 0% (0/3)
  v cancellation_count               100% (3/3)
  v cancellation_rate                100% (3/3)
  v customers_by_country             100% (3/3)
  v avg_items_per_order              100% (3/3)
  X revenue_per_customer               0% (0/3)
  v uk_revenue_share                 100% (3/3)
  v unique_products_sold             100% (3/3)
  v largest_single_order             100% (3/3)
  X orders_per_customer                0% (0/3)
  v negative_amount_count            100% (3/3)

BENCHMARK SUMMARY
  Strategy    Reliability  Coverage  Correct  Fabricated
  sql              70%     100%    42/60       18
```

70% reliability. 18 fabrications. The SQL ran successfully for every question. No errors. No refusals. But 18 answers were wrong.

Here is the problem: you cannot tell which 18 are wrong by looking at the output. The SQL looks reasonable. The numbers are plausible. A data team reviewing these results would have no signal that `avg_order_value`, `monthly_revenue`, `top_products_by_revenue`, `revenue_by_quarter`, `revenue_per_customer`, and `orders_per_customer` are all fabricated.

This is the baseline we are trying to beat.

## Step 5: Run the OM agent

Now run the OM agent strategy. Instead of generating SQL, the agent calls `query_metric` with a metric name and optional filters. The SQL backing each metric was written by a human and tested.

```bash
python run_bench.py \
  --strategies omx_agent \
  --model claude-sonnet-4-6 \
  --iterations 3 \
  --api-url http://localhost:3001
```

```
Strategy: omx_agent (claude-sonnet-4-6)
============================================================
  v total_revenue                    100% (3/3)
  v total_customers                  100% (3/3)
  v total_invoices                   100% (3/3)
  v avg_order_value                  100% (3/3)
  v revenue_by_country_top5          100% (3/3)
  v monthly_revenue                  100% (3/3)
  v churn_count                      100% (3/3)
  v churn_rate                       100% (3/3)
  v top_products_by_revenue          100% (3/3)
  v revenue_by_quarter               100% (3/3)
  v cancellation_count               100% (3/3)
  v cancellation_rate                100% (3/3)
  v customers_by_country             100% (3/3)
  v avg_items_per_order              100% (3/3)
  v revenue_per_customer             100% (3/3)
  v uk_revenue_share                 100% (3/3)
  v unique_products_sold             100% (3/3)
  v largest_single_order             100% (3/3)
  v orders_per_customer              100% (3/3)
  v negative_amount_count            100% (3/3)

BENCHMARK SUMMARY
  Strategy    Reliability  Coverage  Correct  Fabricated
  omx_agent       100%     100%    60/60        0
```

100% reliability. Zero fabrications. 60/60. Every question. Every iteration.

## Step 6: Understanding the journey from 50% to 100%

These results did not happen on our first try. Our first benchmark run on April 10, 2026 scored 50% with the OM agent. Getting from 50% to 100% required fixing four concrete problems. This section documents what those problems were and how we found them.

### The first run: 50%

The initial metric catalog had 11 metrics. The benchmark has 20 questions. The gap was obvious in the failure table:

| Question | Agent picked | Failure reason |
|----------|-------------|----------------|
| How many orders were cancelled? | invoices_count | No cancellation metric existed |
| What percentage are cancellations? | invoices_count | No cancellation rate metric |
| Avg items per order? | avg_amount | Wrong metric (money, not items) |
| How many unique products sold? | (none) | No metric existed |
| Largest single order? | (none) | No metric existed |
| Avg orders per customer? | (none) | No metric existed |
| Revenue per customer? | total_revenue | Wrong metric (total, not per-customer) |
| Top 10 products by revenue? | top_products | Data extraction mismatch |
| Customers per country? | customers_count | No per-country metric |
| Negative invoice count? | invoices_count | No filtered metric existed |

Every failure was diagnostic. The agent either said "I don't have a metric for this" or picked the closest match and got a traceable wrong answer. Zero fabrications even at 50%.

### Fix 1: Add missing metrics

Six questions had no corresponding metric. The fix was straightforward: write the SQL for each one and add it to `metrics.yaml`.

```yaml
# These 6 metrics were missing from the first run

- name: cancellation_count
  description: "Number of cancelled orders."
  sql: "SELECT COUNT(*) AS cancellations FROM invoices WHERE is_cancellation"

- name: cancellation_rate
  description: "Percentage of orders that are cancellations."
  sql: |
    SELECT ROUND(SUM(CASE WHEN is_cancellation THEN 1 ELSE 0 END)
      * 100.0 / COUNT(*), 1) AS cancel_pct FROM invoices

- name: avg_items_per_order
  description: "Average number of line items per order."
  sql: |
    SELECT ROUND(AVG(item_count), 1) AS avg_items
    FROM (SELECT invoice_id, COUNT(*) AS item_count
          FROM invoice_items GROUP BY invoice_id) t

- name: unique_products_sold
  description: "Count of distinct products sold."
  sql: "SELECT COUNT(DISTINCT product_id) AS unique_products
    FROM invoice_items WHERE quantity > 0"

- name: largest_single_order
  description: "Largest single order by total amount in GBP."
  sql: "SELECT MAX(total_amount) AS largest_order
    FROM invoices WHERE NOT is_cancellation"

- name: orders_per_customer
  description: "Average number of orders per customer."
  sql: |
    SELECT ROUND(AVG(order_count), 1) AS avg_orders
    FROM (SELECT customer_id, COUNT(*) AS order_count
          FROM invoices GROUP BY customer_id) t
```

This is the same finding Claire Gouze documented after four weeks of iteration. She called it "enriching the data model." We found it in one benchmark run because the failure table told us exactly which metrics were missing.

### Fix 2: Better metric descriptions

Three questions had the agent pick a similar but wrong metric. "Average items per order" mapped to `avg_amount` (dollar amount) instead of a line-item count. The descriptions were too vague.

Before:

```yaml
- name: avg_amount
  description: "Average invoice amount in GBP"
```

After:

```yaml
- name: avg_amount
  description: "Average invoice amount in GBP. Alias for avg_order_value."

- name: avg_order_value
  description: "Average order value (average invoice amount) in GBP.
    Use for 'average order value', 'avg order value', 'AOV'."
```

And for pre-grouped metrics:

```yaml
- name: top_products
  description: "Top products by revenue with units sold.
    Already grouped by product -- do NOT add a dimension parameter.
    Use for 'top products', 'best selling products', 'top 10 products'."
```

The agent reads these descriptions to select the right metric. Specificity matters. "Average amount" is ambiguous. "Average invoice amount in GBP" is not.

### Fix 3: Benchmark scoring improvements

The data diff function needed two fixes to handle real-world agent behavior:

1. **Extra column tolerance.** A metric may return more columns than the gold SQL expects. The scorer now checks that every gold value appears in the agent's row, allowing extra columns.

2. **Dimension retry.** When the agent adds a dimension parameter to a metric that already has GROUP BY built in, the backend returns 400. The benchmark now retries without the dimension on failure.

These are not cheats. They handle the reality that a governed metric layer returns richer data than a bare SQL query, and that pre-grouped metrics should not fail when the agent redundantly requests grouping.

### Fix 4: Grouping key alignment

The `top_products` metric initially grouped by `(description, stock_code)`, splitting products with multiple SKUs into separate rows. The gold SQL grouped by `description` only.

Both are technically correct SQL. They answer different business questions. GROUP BY description answers "top products by name," which is what a business user asking "top 10 products" wants. GROUP BY description + stock_code answers "top SKUs," an inventory view that is more granular than what was asked.

The benchmark exposed this mismatch immediately. In production without a benchmark, you would discover it when a stakeholder asks "why does your top 10 not match my spreadsheet?" That conversation happens weeks or months later.

The fix: define metrics to match the business question, not the table structure. A governed metric layer forces you to make this decision explicitly at definition time, not at query time.

## Step 7: Run it on your own data

The benchmark is designed to be adapted. To run it on your own warehouse:

1. Write your questions in `questions.yaml`:

```yaml
- name: your_metric_name
  prompt: "Your business question in natural language"
  gold_sql: "SELECT ... (the ground truth query)"
```

2. Define your metrics in `metrics.yaml`

3. Start OnlyMetrix pointing at your config

4. Run:

```bash
python run_bench.py \
  --strategies sql omx_agent \
  --model claude-sonnet-4-6 \
  --iterations 3 \
  --api-url http://localhost:3001
```

The scoring is generic. It works on any dataset, any metric catalog.

Run `verify_setup.py` first to make sure everything is wired up:

```bash
python scripts/verify_setup.py \
  --db-url postgres://localhost:5432/yourdb \
  --api-url http://localhost:3001
```

## What we learned

**Fabrication is the real metric.** Accuracy scores hide silent failures. Track fabrications separately. A system at 70% accuracy with 18 fabrications is more dangerous than one at 50% with zero.

**Agent reliability is a data layer problem.** We went from 50% to 100% without changing the model, the prompt, or the agent architecture. We added missing metrics, improved descriptions, fixed a backend bug, and aligned a grouping key. The agent got smarter because the data layer got more correct.

**The failure table tells you what to fix.** We found 6 missing metrics, 3 bad descriptions, and 1 backend bug in a single benchmark run. Without the structured failure output, you find these by watching the agent fail in production over weeks.

**Documentation matters as much as code.** Three wrong metric selections were fixed by writing better descriptions. The agent reads your docs. Write them like they matter.

**Write tests before fixing bugs.** Six unit tests found the GROUP BY rewrite bug immediately. Without them we would have been guessing at the SQL rewriter's behavior.

## Resources

- [omx-benchmark repo](https://github.com/dreynow/omx-benchmark)
- [Blog post: full results writeup](https://onlymetrix.com/blog/semantic-layer-benchmark)
- [Claire Gouze: How to make Semantic Layer work for Analytics Agents](https://thenewaiorder.substack.com/p/how-to-make-semantic-layer-work-for)
- [dbt Labs: dbt-llm-sl-bench](https://github.com/dbt-labs/dbt-llm-sl-bench)
- [OnlyMetrix docs](https://onlymetrix.com/docs)

OnlyMetrix is open source. `pip install onlymetrix`.
