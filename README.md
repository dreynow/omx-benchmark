# omx-benchmark

Analytics agent benchmark comparing SQL generation vs. governed metrics.

Same methodology as [dbt-labs/dbt-llm-sl-bench](https://github.com/dbt-labs/dbt-llm-sl-bench). 20 business questions, exact data diff scoring, 3 iterations per question.

## Results (April 13, 2026)

```
Strategy                  Reliability  Coverage  Fabrications
SQL baseline (Sonnet 4.6)       70%     100%           18
OM agent (Sonnet 4.6)           98%      98%            0
```

59/60 correct. Zero fabrications. Full writeup: [onlymetrix.com/blog/semantic-layer-benchmark](https://onlymetrix.com/blog/semantic-layer-benchmark)

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

## Running the benchmark

### Prerequisites

- Python 3.10+
- PostgreSQL with UCI retail data loaded (or any SQL-compatible warehouse)
- OnlyMetrix server running (`pip install onlymetrix`)
- Anthropic API key (or OpenAI for GPT models)

### Setup

```bash
git clone https://github.com/dreynow/omx-benchmark
cd omx-benchmark
pip install httpx pyyaml anthropic
```

### Load the data

If you're using the UCI retail dataset with OnlyMetrix:

```bash
pip install onlymetrix
omx connect postgres --url postgres://user:pass@localhost:5432/retail
omx metrics import metrics.yaml
```

### Run

```bash
# Both strategies, 3 iterations, Sonnet
export ANTHROPIC_API_KEY=sk-ant-...
python run_bench.py \
  --strategies sql omx_agent \
  --model claude-sonnet-4-6 \
  --iterations 3 \
  --api-url http://localhost:3001

# Quick single-iteration run
python run_bench.py \
  --strategies omx_agent \
  --model claude-sonnet-4-6 \
  --iterations 1

# GPT-4o baseline
export OPENAI_API_KEY=sk-...
python run_bench.py \
  --strategies sql \
  --model gpt-4o \
  --iterations 3
```

### Output

```
============================================================
Strategy: omx_agent (claude-sonnet-4-6)
============================================================
  v total_revenue                    100% (3/3)
  v total_customers                  100% (3/3)
  v total_invoices                   100% (3/3)
  v avg_order_value                  100% (3/3)
  ...
  v negative_amount_count            100% (3/3)

============================================================
BENCHMARK SUMMARY
============================================================

  Strategy                  Reliability  Coverage  Correct  Fabricated
  ----------------------------------------------------------------
  omx_agent                        98%      98%    59/60        0
```

Results are saved to `benchmark_results.json`.

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
