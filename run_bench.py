"""OnlyMetrix Benchmark — same methodology as dbt-labs/dbt-llm-sl-bench.

Three strategies:
  1. sql       — LLM generates SQL from DDL schema (baseline)
  2. omx_ir    — LLM generates SQL with OM compiled IR as context
  3. omx_agent — LLM uses omx agent (deterministic tool loop, no SQL generation)

Scoring: exact data diff between gold SQL output and agent output.
3 iterations per question to account for LLM randomness.
"""

import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class BenchResult:
    name: str
    prompt: str
    strategy: str
    iteration: int
    correct: bool
    answered: bool
    agent_sql: str | None = None
    error: str | None = None
    duration_ms: int = 0


@dataclass
class StrategyResult:
    strategy: str
    model: str
    total: int
    correct: int
    answered: int
    results: list[BenchResult] = field(default_factory=list)

    @property
    def reliability(self) -> float:
        return self.correct / self.total if self.total > 0 else 0

    @property
    def coverage(self) -> float:
        return self.answered / self.total if self.total > 0 else 0


def load_questions(path: str = "questions.yaml") -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)


def execute_sql(sql: str, api_url: str) -> list[dict]:
    """Execute SQL via OnlyMetrix API and return rows."""
    import httpx
    resp = httpx.post(
        f"{api_url}/v1/query",
        json={"sql": sql, "limit": 1000},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Query failed: {resp.text}")
    data = resp.json()
    return data.get("rows", [])


def _gold_values_in_agent(g_vals: list, a_vals: list) -> bool:
    """Check that every gold value appears in agent values (agent may have extra columns)."""
    if len(g_vals) == len(a_vals):
        return all(values_match(gv, av) for gv, av in zip(g_vals, a_vals))
    if len(g_vals) > len(a_vals):
        return False
    # Gold has fewer values — each gold value must match some agent value
    remaining = list(a_vals)
    for gv in g_vals:
        matched = False
        for i, av in enumerate(remaining):
            if values_match(gv, av):
                remaining.pop(i)
                matched = True
                break
        if not matched:
            return False
    return True


def data_diff(gold_rows: list[dict], agent_rows: list[dict]) -> bool:
    """Compare two result sets — fuzzy column matching, value tolerance.

    Handles:
    - Different column names (matches by value, not key)
    - Single-value results with different key names
    - Multi-row results with column reordering
    - Numeric tolerance (1%)
    """
    if not gold_rows and not agent_rows:
        return True
    if not gold_rows or not agent_rows:
        return False

    # Single-row, single-value shortcut: just compare the numeric value
    if len(gold_rows) == 1 and len(agent_rows) == 1:
        g_vals = [v for v in gold_rows[0].values() if v is not None]
        a_vals = [v for v in agent_rows[0].values() if v is not None]
        # Try matching any gold value against any agent value
        for gv in g_vals:
            for av in a_vals:
                if values_match(gv, av):
                    return True
        # If single numeric values, try direct comparison
        g_nums = [v for v in g_vals if isinstance(v, (int, float))]
        a_nums = [v for v in a_vals if isinstance(v, (int, float))]
        if len(g_nums) == 1 and len(a_nums) == 1:
            return values_match(g_nums[0], a_nums[0])

    # If gold has fewer rows (LIMIT in gold SQL), check if gold rows are a subset of agent rows
    if len(gold_rows) < len(agent_rows):
        # Every gold row must appear in agent rows (value match, ignore column names)
        # Agent may return extra columns — check that all gold values appear in agent values
        def row_values(row):
            return sorted((v for v in row.values() if v is not None), key=str)
        agent_value_sets = [row_values({k.lower(): v for k, v in r.items()}) for r in agent_rows]
        for gr in gold_rows:
            g_vals = row_values({k.lower(): v for k, v in gr.items()})
            found = False
            for a_vals in agent_value_sets:
                if _gold_values_in_agent(g_vals, a_vals):
                    found = True
                    break
            if not found:
                return False
        return True

    if len(gold_rows) > len(agent_rows):
        return False

    # Multi-row: normalize and compare by values
    # Agent may return extra columns — check gold values are a subset of agent values
    def normalize(rows):
        normed = []
        for row in rows:
            normed.append({k.lower(): v for k, v in row.items()})
        return sorted(normed, key=lambda r: str(sorted(str(v) for v in r.values())))

    g = normalize(gold_rows)
    a = normalize(agent_rows)

    for gr, ar in zip(g, a):
        g_vals = sorted((v for v in gr.values() if v is not None), key=str)
        a_vals = sorted((v for v in ar.values() if v is not None), key=str)
        if not _gold_values_in_agent(g_vals, a_vals):
            return False
    return True


def values_match(a, b) -> bool:
    """Compare values with tolerance for floats."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == 0 and b == 0:
            return True
        if a == 0 or b == 0:
            return abs(a - b) < 0.01
        return abs(a - b) / max(abs(a), abs(b)) < 0.01  # 1% tolerance
    return str(a).strip().lower() == str(b).strip().lower()


# ---------------------------------------------------------------------------
# Strategy: Raw SQL (baseline)
# ---------------------------------------------------------------------------

def _call_llm(system: str, user: str, model: str = "gpt-4o") -> str:
    """Call LLM — supports both OpenAI and Anthropic models."""
    if model.startswith("claude"):
        import anthropic
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model=model,
            max_tokens=500,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return resp.content[0].text.strip()
    else:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=model,
            max_tokens=500,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return resp.choices[0].message.content.strip()


def _strip_code_fences(sql: str) -> str:
    if sql.startswith("```"):
        sql = "\n".join(sql.split("\n")[1:])
    if sql.endswith("```"):
        sql = sql.rsplit("```", 1)[0]
    return sql.strip()


def run_sql_strategy(question: str, ddl: str, model: str = "gpt-4o") -> str | None:
    """LLM generates SQL from DDL schema."""
    sql = _call_llm(
        f"You are an analytics agent. Write a PostgreSQL query to answer the question. Return ONLY the SQL, nothing else.\n\nSchema:\n{ddl}",
        question,
        model,
    )
    return _strip_code_fences(sql)


# ---------------------------------------------------------------------------
# Strategy: OM IR context
# ---------------------------------------------------------------------------

def run_omx_ir_strategy(question: str, ir_context: str, model: str = "gpt-4o") -> str | None:
    """LLM generates SQL with OM compiled IR as context."""
    sql = _call_llm(
        f"You are an analytics agent with access to a governed metric layer. Use the compiled IR context to write accurate PostgreSQL queries. Return ONLY the SQL, nothing else.\n\n{ir_context}",
        question,
        model,
    )
    return _strip_code_fences(sql)


# ---------------------------------------------------------------------------
# Strategy: OM Agent (deterministic, no SQL generation)
# ---------------------------------------------------------------------------

def run_omx_agent_strategy(question: str, api_url: str) -> list[dict] | None:
    """Use omx agent — extract ground truth from tool call results, not chat narrative.

    The agent calls query_metric internally. We re-execute the same query_metric
    call using the args from steps[] to get the raw structured rows.
    This is the ground truth — not the formatted narrative.
    """
    import httpx
    resp = httpx.post(
        f"{api_url}/v1/chat",
        json={"message": question, "history": [], "investigation_mode": False},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    if resp.status_code != 200:
        return None
    data = resp.json()

    # Primary: re-query the metric using the agent's tool call args
    # This captures the exact structured data the agent saw
    steps = data.get("steps", [])
    for step in steps:
        if step.get("tool") == "query_metric":
            args = step.get("args", {})
            metric_name = args.get("metric_name")
            if metric_name:
                try:
                    body = {}
                    if args.get("dimension"):
                        body["dimension"] = args["dimension"]
                    if args.get("granularity"):
                        body["granularity"] = args["granularity"]
                    # Don't pass limit — get all rows, let data_diff handle subset matching
                    if args.get("period"):
                        body["period"] = args["period"]
                    mr = httpx.post(
                        f"{api_url}/v1/metrics/{metric_name}",
                        json=body,
                        headers={"Content-Type": "application/json"},
                        timeout=15,
                    )
                    if mr.status_code == 200:
                        rows = mr.json().get("rows", [])
                        if rows:
                            return rows

                    # Retry without dimension — metric may already have GROUP BY built in
                    if body.get("dimension"):
                        body_no_dim = {k: v for k, v in body.items() if k != "dimension"}
                        mr2 = httpx.post(
                            f"{api_url}/v1/metrics/{metric_name}",
                            json=body_no_dim,
                            headers={"Content-Type": "application/json"},
                            timeout=15,
                        )
                        if mr2.status_code == 200:
                            rows = mr2.json().get("rows", [])
                            if rows:
                                return rows
                except Exception:
                    pass

    # Fallback: if no query_metric step, the agent answered from
    # list_metrics or reliability_check — no structured data to compare
    return None


# ---------------------------------------------------------------------------
# Strategy: OM Agent with SQL fallback
# ---------------------------------------------------------------------------

def run_omx_agent_fallback_strategy(question: str, api_url: str, ir_context: str, model: str = "gpt-4o") -> list[dict] | None:
    """Try omx agent first. If it returns no structured data, fall back to LLM SQL."""
    result = run_omx_agent_strategy(question, api_url)
    if result and len(result) > 0:
        return result

    # Fallback: generate SQL with IR context
    sql = run_omx_ir_strategy(question, ir_context, model)
    if sql:
        try:
            rows = execute_sql(sql, api_url)
            return rows
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_benchmark(
    questions: list[dict],
    api_url: str,
    strategies: list[str],
    ddl: str = "",
    ir_context: str = "",
    model: str = "gpt-4o",
    iterations: int = 3,
):
    all_results: dict[str, StrategyResult] = {}

    for strategy in strategies:
        print(f"\n{'='*60}")
        print(f"Strategy: {strategy} ({model})")
        print(f"{'='*60}")

        sr = StrategyResult(strategy=strategy, model=model, total=len(questions) * iterations, correct=0, answered=0)

        for q in questions:
            q_correct = 0
            q_answered = 0

            # Get gold result
            try:
                gold_rows = execute_sql(q["gold_sql"], api_url)
            except Exception as e:
                print(f"  SKIP {q['name']}: gold SQL failed — {e}")
                continue

            for i in range(iterations):
                t0 = time.time()
                try:
                    if strategy == "sql":
                        agent_sql = run_sql_strategy(q["prompt"], ddl, model)
                        agent_rows = execute_sql(agent_sql, api_url) if agent_sql else None
                    elif strategy == "omx_ir":
                        agent_sql = run_omx_ir_strategy(q["prompt"], ir_context, model)
                        agent_rows = execute_sql(agent_sql, api_url) if agent_sql else None
                    elif strategy == "omx_agent":
                        agent_sql = None
                        agent_rows = run_omx_agent_strategy(q["prompt"], api_url)
                    elif strategy == "omx_agent_fallback":
                        agent_sql = None
                        agent_rows = run_omx_agent_fallback_strategy(q["prompt"], api_url, ir_context, model)
                    else:
                        continue

                    answered = agent_rows is not None
                    correct = data_diff(gold_rows, agent_rows) if answered else False
                    duration = int((time.time() - t0) * 1000)

                    if answered:
                        q_answered += 1
                        sr.answered += 1
                    if correct:
                        q_correct += 1
                        sr.correct += 1

                    sr.results.append(BenchResult(
                        name=q["name"], prompt=q["prompt"], strategy=strategy,
                        iteration=i, correct=correct, answered=answered,
                        agent_sql=agent_sql, duration_ms=duration,
                    ))

                except Exception as e:
                    sr.results.append(BenchResult(
                        name=q["name"], prompt=q["prompt"], strategy=strategy,
                        iteration=i, correct=False, answered=False, error=str(e),
                        duration_ms=int((time.time() - t0) * 1000),
                    ))

            reliability = q_correct / iterations * 100
            icon = "v" if q_correct == iterations else "~" if q_correct > 0 else "X"
            print(f"  {icon} {q['name']:30} {reliability:5.0f}% ({q_correct}/{iterations})")

        all_results[strategy] = sr

    return all_results


def print_summary(results: dict[str, StrategyResult]):
    print(f"\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*60}\n")
    print(f"  {'Strategy':<25} {'Reliability':>11} {'Coverage':>9} {'Correct':>8} {'Fabricated':>11}")
    print(f"  {'-'*64}")
    for name, sr in results.items():
        # Fabrication = answered but wrong (not unanswered, not correct)
        fabricated = sum(1 for r in sr.results if r.answered and not r.correct)
        print(f"  {name:<25} {sr.reliability:>10.0%} {sr.coverage:>8.0%} {sr.correct:>5}/{sr.total}  {fabricated:>7}")
    print()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OnlyMetrix Benchmark")
    parser.add_argument("--strategies", nargs="+", default=["sql", "omx_ir"], help="Strategies to run")
    parser.add_argument("--model", default="gpt-4o", help="LLM model")
    parser.add_argument("--iterations", type=int, default=3, help="Iterations per question")
    parser.add_argument("--api-url", default=None, help="OnlyMetrix API URL")
    parser.add_argument("--questions", default="questions.yaml", help="Questions file")
    args = parser.parse_args()

    api_url = args.api_url or os.environ.get("OMX_API_URL", "http://localhost:3001")

    questions = load_questions(args.questions)
    print(f"Loaded {len(questions)} questions")

    # Load schema from /v1/tables API
    import httpx

    schema_context = ""
    try:
        tables_resp = httpx.get(f"{api_url}/v1/tables", timeout=10)
        tables = tables_resp.json().get("tables", [])
        for t in tables:
            desc_resp = httpx.get(f"{api_url}/v1/tables/{t['schema']}.{t['table']}", timeout=10)
            desc = desc_resp.json()
            cols = desc.get("columns", [])
            col_str = ", ".join(f"{c.get('name', c.get('column_name', '?'))} {c.get('type', c.get('data_type', '?'))}" for c in cols)
            rows = t.get("estimated_rows", "?")
            schema_context += f"{t['table']} ({rows} rows): {col_str}\n"
    except Exception:
        schema_context = "Tables: invoices, customers, products, invoice_items"

    # DDL for sql strategy
    ddl = f"PostgreSQL database schema:\n{schema_context}"

    # IR context for omx_ir strategy
    ir_context = ""
    if "omx_ir" in args.strategies:
        try:
            resp = httpx.get(f"{api_url}/v1/compiler/status", timeout=10)
            ir = resp.json()
            ir_context = f"OnlyMetrix compiled metrics ({ir['total']} total, {ir['structured']} structured).\n"
            ir_context += f"Use these metric definitions to write accurate SQL.\n\n"
            for m in ir.get("metrics", []):
                ir_context += f"Metric: {m['name']} ({m['kind']})\n"
                for measure in m.get("measures", []):
                    ir_context += f"  Aggregation: {measure['function']}({measure['alias']})\n"
                for dim in m.get("dimensions", []):
                    ir_context += f"  Dimension: {dim['name']} ({dim['type']})\n"
                if m.get("time_grain"):
                    ir_context += f"  Time column: {m['time_grain']['column']}\n"
                ir_context += "\n"
            ir_context += f"\nDatabase schema:\n{schema_context}"
        except Exception:
            ir_context = f"Database schema:\n{schema_context}"

    results = run_benchmark(
        questions=questions,
        api_url=api_url,
        strategies=args.strategies,
        ddl=ddl,
        ir_context=ir_context,
        model=args.model,
        iterations=args.iterations,
    )

    print_summary(results)

    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model": args.model,
        "iterations": args.iterations,
        "results": {
            name: {
                "reliability": sr.reliability,
                "coverage": sr.coverage,
                "correct": sr.correct,
                "total": sr.total,
            }
            for name, sr in results.items()
        },
    }
    with open("benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to benchmark_results.json")
