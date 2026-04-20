# omx-benchmark

Analytics agent benchmark comparing SQL generation vs. governed metrics. 100% reliability, 0 fabrications. 60/60 correct.

Same methodology as [dbt-labs/dbt-llm-sl-bench](https://github.com/dbt-labs/dbt-llm-sl-bench). 20 business questions, exact data diff scoring, 3 iterations per question.

## Results (April 20, 2026)

```
Strategy                  Reliability  Coverage  Fabrications
SQL baseline (Sonnet 4.6)       70%     100%           18
OM agent (Sonnet 4.6)          100%     100%            0
```

60/60 correct across 20 questions × 3 iterations. Zero fabricated answers. Every question answered. Verified stable across **three independent 3-iteration runs** against a Snowflake-backed `ONLYMETRIX_BENCHMARK` workspace on `api.onlymetrix.com`.

Full writeup: [onlymetrix.com/blog/semantic-layer-benchmark](https://onlymetrix.com/blog/semantic-layer-benchmark)

## What's measured

**Reliability** = correct answers / total questions.

**Coverage** = questions answered / total questions (agent may refuse).

**Fabrications** = agent returned a plausible wrong number (not a refusal, not an error).

This last metric is what matters for production. A system that scores 70% with 18 hidden fabrications is less useful than one that scores 50% with honest failures.

## Dataset

UCI Online Retail dataset. Real e-commerce transaction data.

| Table | Rows | Description |
|-------|------|-------------|
| invoices | ~45K | Sales invoices with amounts, dates, countries |
| customers | ~6K | Customer records with churn flags |
| products | ~5K | Product catalog |
| invoice_items | ~825K | Line items with quantities and prices |

### Why UCI Online Retail, not ACME Insurance

dbt Labs used the ACME Insurance dataset with 43 questions about policies, claims, and loss ratios. Their dbt models were built specifically to answer those questions. We chose UCI Online Retail because:

1. **Real transaction data**, not a benchmark dataset designed for testing
2. **Questions written before the metric layer was built**. No circular advantage.
3. **Reflects the kind of data OM users actually work with**: e-commerce, revenue, customers, products

The comparison is methodology, not dataset. Both benchmarks measure the same thing: can an agent answer business questions correctly, without fabricating answers?

## Questions

20 business questions in `questions.yaml`, spanning:

- Simple KPIs (total revenue, customer count)
- Dimension breakdowns (revenue by country, customers by country)
- Time series (monthly revenue, quarterly revenue)
- Derived calculations (revenue per customer, avg items per order)
- Filtered aggregations (cancellation rate, churn rate)
- Multi-table joins (top 10 products by revenue)

Each question includes gold SQL that produces the ground truth result.

## Strategies

### SQL baseline (`sql`)

The LLM receives the full database schema and generates SQL from scratch. Standard text-to-SQL.

### OM agent (`omx_agent`)

The LLM calls `query_metric` with a metric name and optional filters. The SQL backing each metric is pre-defined and tested. The LLM never writes SQL.

### OM agent with fallback (`omx_agent_fallback`)

Tries the OM agent first. If the metric doesn't exist, falls back to LLM-generated SQL with IR context.

### OM IR context (`omx_ir`)

The LLM generates SQL but receives OnlyMetrix compiled IR (metric definitions, dimensions, relationships) as additional context.

## Reproducing this benchmark

The result above came from a workspace on `api.onlymetrix.com` with the UCI Online Retail dataset loaded in Snowflake (5,942 customers / 44,876 invoices / 824,364 line items / 4,646 products). To reproduce, stand up an equivalent workspace — two options below.

### Option 1 — OnlyMetrix Cloud (sign-up, ~10 minutes)

1. Sign up at [app.onlymetrix.com](https://app.onlymetrix.com) (free tier).
2. Load UCI Online Retail into a Postgres or Snowflake you own. A loader script is in this repo:
   ```bash
   git clone https://github.com/dreynow/omx-benchmark
   cd omx-benchmark
   python scripts/load_data.py --db-url <your-warehouse-url>
   ```
3. In the dashboard → Datasources → connect that warehouse as `default`.
4. In the dashboard → Settings → Allowed Schemas → add `public`.
5. Import the benchmark's curated metrics:
   ```bash
   pip install onlymetrix
   export OMX_API_URL=https://api.onlymetrix.com
   export OMX_API_KEY=<your-workspace-key>
   omx metrics import metrics.yaml
   ```
6. For time-series questions (`monthly_revenue`, `revenue_by_quarter`), the UCI data ends 2011-12-09. Anchor the server's "now" to that date so `last_12_months` resolves against the data instead of calendar time:
   ```bash
   curl -X POST -H "Authorization: Bearer $OMX_API_KEY" -H "Content-Type: application/json" \
     -d '{"allowed_schemas":["public"], "time_now_override":"2011-12-09T00:00:00Z"}' \
     https://api.onlymetrix.com/v1/setup/configure-access
   ```
7. Run the benchmark:
   ```bash
   pip install httpx pyyaml anthropic
   export ANTHROPIC_API_KEY=sk-ant-...
   python run_bench.py \
     --api-url https://api.onlymetrix.com \
     --strategies omx_agent \
     --model claude-sonnet-4-6 \
     --iterations 3
   ```

### Option 2 — Self-hosted (local OM server + Postgres)

If you'd rather not sign up, run the whole stack locally:

```bash
# 1. Postgres with UCI retail loaded
docker run -d --name om-pg -e POSTGRES_PASSWORD=retail -p 5432:5432 postgres:16
python scripts/load_data.py --db-url postgres://postgres:retail@localhost:5432/postgres

# 2. OnlyMetrix server (cargo build or pre-built binary)
#    Config expects `datasets/retail/config.yaml` style self-hosted config.
#    See https://github.com/dreynow/onlymetrix-python for details.
omx-server --config config.yaml &

# 3. Register metrics
omx metrics import metrics.yaml

# 4. Run benchmark
export ANTHROPIC_API_KEY=sk-ant-...
python run_bench.py \
  --api-url http://localhost:3001 \
  --strategies omx_agent \
  --model claude-sonnet-4-6 \
  --iterations 3
```

### Expected output

```
============================================================
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
  ...  (all 20 questions)

============================================================
BENCHMARK SUMMARY
============================================================

  Strategy                  Reliability  Coverage  Correct  Fabricated
  ----------------------------------------------------------------
  omx_agent                       100%     100%    60/60        0
```

Results are written to `benchmark_results.json`.

### Baseline (raw SQL) for comparison

```bash
python run_bench.py \
  --strategies sql \
  --model claude-sonnet-4-6 \
  --iterations 3 \
  --api-url <whichever-endpoint-you-used>
```

You supply your own Anthropic (or OpenAI) key. Expected: ~70% reliability with ~18 fabrications across the 60 (answer, iteration) pairs.

## Scoring

Exact data diff with:

- **Column-name agnostic**: matches by value, not key name
- **1% numeric tolerance**: `abs(a - b) / max(abs(a), abs(b)) < 0.01`
- **Subset matching**: if gold has LIMIT 5, checks those 5 rows appear in agent results
- **Extra column tolerance**: agent may return more columns than gold (e.g., stock_code alongside revenue)

No LLM judge. No semantic matching. Correct = exact match. Wrong = wrong.

## Adapting to your data

1. Replace `questions.yaml` with your own questions and gold SQL
2. Update `metrics.yaml` with your metric definitions
3. Point `--api-url` at your OnlyMetrix server
4. Run the benchmark

The scoring logic is generic. The questions are templates you can adapt.

## Related work

- [Claire Gouze: How to make Semantic Layer work for Analytics Agents](https://thenewaiorder.substack.com/p/how-to-make-semantic-layer-work-for) (13% to 82%, 4 weeks)
- [dbt Labs: Semantic Layer vs. Text-to-SQL benchmark](https://github.com/dbt-labs/dbt-llm-sl-bench) (ACME Insurance, 43 questions, 3 strategies)
- [OnlyMetrix blog: full writeup of these results](https://onlymetrix.com/blog/semantic-layer-benchmark)

## License

MIT
