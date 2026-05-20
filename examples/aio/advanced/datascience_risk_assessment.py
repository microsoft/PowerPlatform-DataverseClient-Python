# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Async Data Science Risk Assessment Pipeline

Async equivalent of examples/advanced/datascience_risk_assessment.py.

End-to-end example: Extract Dataverse data concurrently into DataFrames,
run statistical analysis, generate LLM-powered risk summaries, and write
results back to Dataverse -- a realistic data analyst / data scientist workflow.

Pipeline flow:
    Dataverse SDK (async) --> Pandas DataFrame --> Analysis + LLM --> Write-back & Reports

The three Dataverse extraction queries (accounts, cases, opportunities) run
concurrently via asyncio.gather(), reducing wall-clock time for the extract step.

Scenario:
    A financial services company tracks customer accounts, service cases, and
    revenue opportunities in Dataverse. The risk team needs to:
    1) Pull data from multiple tables into DataFrames (concurrently)
    2) Compute risk scores using statistical analysis (pandas/numpy)
    3) Classify and summarize risk using an LLM
    4) Write risk assessments back to Dataverse
    5) Produce a summary report

    Note: This example reads from existing Dataverse tables (account,
    incident, opportunity) and does not create or delete any tables.
    Step 4 (write-back) is disabled by default -- uncomment it in
    run_risk_pipeline() to write risk scores back to account records.

Prerequisites (required -- included in SDK dependencies):
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity

Additional libraries (optional -- used for visualization and LLM; not part
of the SDK and must be installed separately. Pick ONE LLM provider):
    pip install matplotlib          # for charts / visualization
    pip install azure-ai-inference   # Option A: Azure AI Foundry / Azure OpenAI
    pip install openai               # Option B: OpenAI / Azure OpenAI
    pip install github-copilot-sdk   # Option C: GitHub Copilot SDK (requires Copilot CLI)
"""

import asyncio
import sys
import warnings
from pathlib import Path
from textwrap import dedent

# Suppress MSAL advisory about response_mode (third-party library, not actionable here)
warnings.filterwarnings("ignore", message="response_mode=.*form_post", category=UserWarning)

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.models.filters import col, raw

# -- Optional imports (graceful degradation if not installed) ------

try:
    import matplotlib

    matplotlib.use("Agg")  # non-interactive backend (no GUI required)
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


# ================================================================
# LLM Provider Configuration
# ================================================================
# Same providers as the sync version. LLM calls are kept synchronous
# here since they are CPU-light blocking calls. Replace with async
# LLM clients (e.g. openai.AsyncOpenAI) if latency matters.


def get_llm_client(provider=None, endpoint=None, api_key=None, model="gpt-4o"):
    """Create an LLM client using the specified (or first available) provider.

    Returns a callable: llm_complete(system_prompt, user_prompt) -> str
    Returns None if no provider is available.
    """
    providers = [provider] if provider else ["azure-ai-inference", "openai", "copilot-sdk"]
    for p in providers:
        client = _try_init_provider(p, endpoint, api_key, model)
        if client is not None:
            return client
    return None


def _wrap_with_logging(raw_complete, provider_name, model_name):
    import time

    log = []

    def complete(system_prompt, user_prompt):
        start = time.time()
        response = raw_complete(system_prompt, user_prompt)
        elapsed = time.time() - start
        log.append(
            {
                "provider": provider_name,
                "model": model_name,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "response": response,
                "elapsed_seconds": round(elapsed, 2),
            }
        )
        return response

    complete.log = log
    complete.provider_name = provider_name
    complete.model_name = model_name
    return complete


def _try_init_provider(name, endpoint, api_key, model):
    if name == "azure-ai-inference":
        return _init_azure_ai(endpoint, api_key, model)
    elif name == "openai":
        return _init_openai(endpoint, api_key, model)
    elif name == "copilot-sdk":
        return _init_copilot_sdk()
    return None


def _init_azure_ai(endpoint, api_key, model):
    try:
        from azure.ai.inference import ChatCompletionsClient
        from azure.ai.inference.models import SystemMessage, UserMessage
        from azure.core.credentials import AzureKeyCredential
    except ImportError:
        return None

    if not endpoint or not api_key:
        return None

    client = ChatCompletionsClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))

    def complete(system_prompt, user_prompt):
        response = client.complete(
            messages=[SystemMessage(content=system_prompt), UserMessage(content=user_prompt)],
            max_tokens=150,
            temperature=0.3,
        )
        return response.choices[0].message.content.strip()

    print("[INFO] LLM provider: Azure AI Inference")
    return _wrap_with_logging(complete, "Azure AI Inference", model)


def _init_openai(endpoint, api_key, model):
    try:
        import openai
    except ImportError:
        return None

    if not api_key:
        return None

    if endpoint:
        client = openai.AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version="2024-02-01")
    else:
        client = openai.OpenAI(api_key=api_key)

    def complete(system_prompt, user_prompt):
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=150,
            temperature=0.3,
        )
        return response.choices[0].message.content.strip()

    provider_name = "Azure OpenAI" if endpoint else "OpenAI"
    print(f"[INFO] LLM provider: {provider_name}")
    return _wrap_with_logging(complete, provider_name, model)


def _init_copilot_sdk():
    # Uncomment and configure to use your Copilot subscription as the LLM provider.
    # from copilot import CopilotClient
    # ...
    return None


# ================================================================
# Configuration
# ================================================================

TABLE_ACCOUNTS = "account"
TABLE_CASES = "incident"
TABLE_OPPORTUNITIES = "opportunity"

RISK_HIGH = 75
RISK_MEDIUM = 40

_SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = _SCRIPT_DIR / "risk_assessment_output"


async def main():
    """Entry point -- authenticate and run the async pipeline."""
    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("[ERR] No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    print("[INFO] Authenticating via browser...")
    credential = AsyncInteractiveBrowserCredential()
    try:
        async with AsyncDataverseClient(base_url, credential) as client:
            await run_risk_pipeline(client)
    finally:
        await credential.close()


# ================================================================
# Step 1: Extract -- Pull data concurrently with asyncio.gather
# ================================================================


async def step1_extract(client):
    """Extract accounts, cases, and opportunities concurrently."""
    print("\n" + "=" * 60)
    print("STEP 1: Extract data from Dataverse (concurrently)")
    print("=" * 60)

    # All three queries run in parallel -- significant speedup vs sequential.
    accounts_result, cases_result, opps_result = await asyncio.gather(
        client.query.builder(TABLE_ACCOUNTS)
        .select("accountid", "name", "revenue", "numberofemployees", "industrycode")
        .where(col("statecode") == 0)
        .top(200)
        .execute(),
        client.query.builder(TABLE_CASES)
        .select("incidentid", "_customerid_value", "title", "severitycode", "prioritycode", "createdon")
        .where(raw("statecode eq 0"))
        .top(1000)
        .execute(),
        client.query.builder(TABLE_OPPORTUNITIES)
        .select(
            "opportunityid",
            "_parentaccountid_value",
            "name",
            "estimatedvalue",
            "closeprobability",
            "estimatedclosedate",
        )
        .where(col("statecode") == 0)
        .top(1000)
        .execute(),
    )

    accounts = accounts_result.to_dataframe()
    cases = cases_result.to_dataframe()
    opportunities = opps_result.to_dataframe()

    print(f"[OK] Extracted {len(accounts)} active accounts")
    print(f"[OK] Extracted {len(cases)} open cases")
    print(f"[OK] Extracted {len(opportunities)} active opportunities")

    return accounts, cases, opportunities


# ================================================================
# Step 2: Transform & Analyze -- Statistical risk scoring
# ================================================================


def step2_analyze(accounts, cases, opportunities):
    """Compute risk scores using pandas statistical operations (pure Python, unchanged)."""
    print("\n" + "=" * 60)
    print("STEP 2: Statistical analysis -- compute risk scores")
    print("=" * 60)

    if not cases.empty and "_customerid_value" in cases.columns:
        case_stats = (
            cases.groupby("_customerid_value")
            .agg(
                total_cases=("incidentid", "count"),
                high_severity_cases=("severitycode", lambda x: (x == 1).sum()),
                avg_priority=("prioritycode", "mean"),
            )
            .reset_index()
            .rename(columns={"_customerid_value": "accountid"})
        )
    else:
        case_stats = pd.DataFrame(columns=["accountid", "total_cases", "high_severity_cases", "avg_priority"])

    if not opportunities.empty and "_parentaccountid_value" in opportunities.columns:
        opportunities = opportunities.copy()
        opportunities["_weighted_value"] = (
            pd.to_numeric(opportunities["estimatedvalue"], errors="coerce").fillna(0)
            * pd.to_numeric(opportunities["closeprobability"], errors="coerce").fillna(0)
            / 100
        )
        opp_stats = (
            opportunities.groupby("_parentaccountid_value")
            .agg(
                total_opportunities=("opportunityid", "count"),
                pipeline_value=("estimatedvalue", "sum"),
                avg_close_probability=("closeprobability", "mean"),
                weighted_pipeline=("_weighted_value", "sum"),
            )
            .reset_index()
            .rename(columns={"_parentaccountid_value": "accountid"})
        )
    else:
        opp_stats = pd.DataFrame(
            columns=[
                "accountid",
                "total_opportunities",
                "pipeline_value",
                "avg_close_probability",
                "weighted_pipeline",
            ]
        )

    risk_df = accounts.merge(case_stats, on="accountid", how="left")
    risk_df = risk_df.merge(opp_stats, on="accountid", how="left")

    for c in ["revenue", "numberofemployees"]:
        if c in risk_df.columns:
            risk_df[c] = pd.to_numeric(risk_df[c], errors="coerce").fillna(0)

    for c in ["total_cases", "high_severity_cases"]:
        risk_df[c] = pd.to_numeric(risk_df[c], errors="coerce").fillna(0).astype(int)
    for c in ["avg_priority", "pipeline_value", "avg_close_probability", "weighted_pipeline"]:
        risk_df[c] = pd.to_numeric(risk_df[c], errors="coerce").fillna(0).astype(float)
    risk_df["total_opportunities"] = (
        pd.to_numeric(risk_df["total_opportunities"], errors="coerce").fillna(0).astype(int)
    )

    risk_df["risk_score"] = compute_risk_score(risk_df)
    risk_df["risk_tier"] = risk_df["risk_score"].apply(classify_risk)

    print(f"[OK] Computed risk scores for {len(risk_df)} accounts")
    print(f"  High risk:   {(risk_df['risk_tier'] == 'High').sum()}")
    print(f"  Medium risk: {(risk_df['risk_tier'] == 'Medium').sum()}")
    print(f"  Low risk:    {(risk_df['risk_tier'] == 'Low').sum()}")

    print("\n  Risk score distribution:")
    print(f"    Mean:   {risk_df['risk_score'].mean():.1f}")
    print(f"    Median: {risk_df['risk_score'].median():.1f}")
    print(f"    Std:    {risk_df['risk_score'].std():.1f}")
    print(f"    Min:    {risk_df['risk_score'].min():.1f}")
    print(f"    Max:    {risk_df['risk_score'].max():.1f}")

    return risk_df


def compute_risk_score(df):
    """Compute a 0-100 risk score from multiple factors."""
    scores = pd.Series(0.0, index=df.index)

    case_total = df["total_cases"].clip(lower=1)
    severity_ratio = df["high_severity_cases"] / case_total
    scores += severity_ratio * 35

    if df["total_cases"].max() > 0:
        case_pctile = df["total_cases"].rank(pct=True)
        scores += case_pctile * 25
    else:
        scores += 12.5

    max_pipeline = df["weighted_pipeline"].max()
    if max_pipeline > 0:
        pipeline_strength = df["weighted_pipeline"] / max_pipeline
        scores += (1 - pipeline_strength) * 20
    else:
        scores += 10

    close_risk = (100 - df["avg_close_probability"]) / 100
    scores += close_risk * 20

    return scores.clip(0, 100).round(1)


def classify_risk(score):
    if score >= RISK_HIGH:
        return "High"
    elif score >= RISK_MEDIUM:
        return "Medium"
    return "Low"


# ================================================================
# Step 3: LLM Summarization
# ================================================================


def step3_summarize(risk_df, llm_complete=None):
    """Generate per-account risk summaries using LLM or template fallback."""
    print("\n" + "=" * 60)
    print("STEP 3: Generate risk summaries")
    print("=" * 60)

    flagged = risk_df[risk_df["risk_tier"].isin(["High", "Medium"])].copy()
    print(f"[INFO] Generating summaries for {len(flagged)} flagged accounts")

    if llm_complete is not None:
        summaries = _summarize_with_llm(flagged, llm_complete)
        if hasattr(llm_complete, "log") and llm_complete.log:
            _export_llm_log(llm_complete)
    else:
        print("[INFO] No LLM provider configured -- using template-based summarization")
        summaries = _summarize_with_template(flagged)

    flagged["risk_summary"] = summaries
    summary_map = dict(zip(flagged["accountid"], flagged["risk_summary"]))
    risk_df["risk_summary"] = risk_df["accountid"].map(summary_map).fillna("Low risk -- no action needed.")

    print(f"[OK] Generated {len(summaries)} risk summaries")

    top_risk = risk_df.nlargest(3, "risk_score")
    for _, row in top_risk.iterrows():
        print(f"\n  Account: {row.get('name', 'Unknown')}")
        print(f"  Risk Score: {row['risk_score']} ({row['risk_tier']})")
        print(f"  Summary: {row['risk_summary'][:120]}...")

    return risk_df


def _summarize_with_llm(flagged_df, llm_complete):
    system_prompt = (
        "You are a customer risk analyst at a financial services company. "
        "Write exactly 2-3 sentences per account. "
        "Sentence 1: State the risk level and primary driver. "
        "Sentence 2: Quantify the key metric(s) behind the risk. "
        "Sentence 3 (if needed): Recommend one specific action. "
        "Use plain business language. Do not use bullet points or markdown."
    )

    summaries = []
    for _, row in flagged_df.iterrows():
        user_prompt = dedent(f"""\
            Summarize the risk for this account:

            Account Name: {row.get("name", "Unknown")}
            Risk Score: {row["risk_score"]:.0f}/100 ({row["risk_tier"]} risk)
            Open Support Cases: {row["total_cases"]} total, {row["high_severity_cases"]} high-severity
            Revenue Pipeline: ${row["pipeline_value"]:,.0f} total, ${row["weighted_pipeline"]:,.0f} probability-weighted
            Average Deal Close Probability: {row["avg_close_probability"]:.0f}%
        """)
        summaries.append(llm_complete(system_prompt, user_prompt))

    return summaries


def _summarize_with_template(flagged_df):
    summaries = []
    for _, row in flagged_df.iterrows():
        name = row.get("name", "Unknown")
        parts = []

        if row["high_severity_cases"] > 0:
            parts.append(f"{row['high_severity_cases']} high-severity cases require immediate attention")
        if row["total_cases"] > 5:
            parts.append(f"elevated case volume ({row['total_cases']} open)")
        if row["weighted_pipeline"] < 10000:
            parts.append("weak revenue pipeline")
        if row["avg_close_probability"] < 30:
            parts.append(f"low close probability ({row['avg_close_probability']:.0f}%)")
        if not parts:
            parts.append("multiple moderate risk factors detected")

        summary = (
            f"{name} has a {row['risk_tier'].lower()} risk score of "
            f"{row['risk_score']:.0f}/100. Key factors: {'; '.join(parts)}. "
            f"Recommend proactive outreach and account review."
        )
        summaries.append(summary)

    return summaries


def _export_llm_log(llm_complete, include_prompts=False):
    log_path = OUTPUT_DIR / "llm_interactions.txt"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("LLM Interaction Log\n")
        f.write("=" * 70 + "\n")
        f.write(f"Provider: {llm_complete.provider_name}\n")
        f.write(f"Model: {llm_complete.model_name}\n")
        f.write(f"Total calls: {len(llm_complete.log)}\n")
        total_time = sum(entry["elapsed_seconds"] for entry in llm_complete.log)
        f.write(f"Total time: {total_time:.1f}s\n")
        f.write("=" * 70 + "\n\n")

        for i, entry in enumerate(llm_complete.log, 1):
            f.write(f"--- Call {i} ({entry['elapsed_seconds']:.2f}s) ---\n\n")
            if include_prompts:
                f.write(f"[System Prompt]\n{entry['system_prompt']}\n\n")
                f.write(f"[User Prompt]\n{entry['user_prompt']}\n\n")
                f.write(f"[Response]\n{entry['response']}\n\n")
            else:
                f.write(f"[Response length: {len(entry['response'])} chars]\n\n")

    print(f"[OK] LLM interaction log saved to {log_path}")


# ================================================================
# Step 4: Write-back
# ================================================================


async def step4_writeback(client, risk_df):
    """Write risk scores and summaries back to Dataverse accounts."""
    print("\n" + "=" * 60)
    print("STEP 4: Write risk assessments back to Dataverse")
    print("=" * 60)

    update_df = risk_df[["accountid", "description"]].copy()
    update_df["description"] = risk_df.apply(
        lambda r: f"[Risk: {r['risk_tier']} ({r['risk_score']:.0f}/100)] {r['risk_summary']}",
        axis=1,
    )

    await client.dataframe.update(TABLE_ACCOUNTS, update_df, id_column="accountid")
    print(f"[OK] Updated {len(update_df)} account records with risk assessments")


# ================================================================
# Step 5: Report
# ================================================================


def step5_report(risk_df):
    """Generate a summary report with optional visualization."""
    print("\n" + "=" * 60)
    print("STEP 5: Risk assessment report")
    print("=" * 60)

    tier_summary = (
        risk_df.groupby("risk_tier")
        .agg(
            count=("accountid", "count"),
            avg_score=("risk_score", "mean"),
            total_cases=("total_cases", "sum"),
            total_pipeline=("pipeline_value", "sum"),
        )
        .round(1)
    )
    print("\nRisk Tier Summary:")
    print(tier_summary.to_string())

    top10 = risk_df.nlargest(10, "risk_score")[
        ["name", "risk_score", "risk_tier", "total_cases", "high_severity_cases", "pipeline_value"]
    ]
    print("\nTop 10 Highest Risk Accounts:")
    print(top10.to_string(index=False))

    if HAS_MATPLOTLIB:
        _generate_charts(risk_df)
    else:
        print("\n[INFO] Install matplotlib for risk visualization charts")

    risk_df.to_csv(OUTPUT_DIR / "risk_scores.csv", index=False)
    top10.to_csv(OUTPUT_DIR / "top10_risk.csv", index=False)
    tier_summary.to_csv(OUTPUT_DIR / "tier_summary.csv")
    print(f"\n[OK] Exported CSV reports to {OUTPUT_DIR}/")

    print("\n[OK] Risk assessment pipeline complete!")


def _generate_charts(risk_df):
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Customer Account Risk Assessment", fontsize=14, fontweight="bold")

    axes[0].hist(risk_df["risk_score"], bins=20, color="#4472C4", edgecolor="white")
    axes[0].axvline(RISK_HIGH, color="red", linestyle="--", label=f"High ({RISK_HIGH})")
    axes[0].axvline(RISK_MEDIUM, color="orange", linestyle="--", label=f"Medium ({RISK_MEDIUM})")
    axes[0].set_title("Risk Score Distribution")
    axes[0].set_xlabel("Risk Score")
    axes[0].set_ylabel("Number of Accounts")
    axes[0].legend()

    tier_counts = risk_df["risk_tier"].value_counts()
    colors = {"High": "#FF4444", "Medium": "#FFA500", "Low": "#44BB44"}
    axes[1].pie(
        tier_counts.values,
        labels=tier_counts.index,
        colors=[colors.get(t, "#888") for t in tier_counts.index],
        autopct="%1.0f%%",
        startangle=90,
    )
    axes[1].set_title("Risk Tier Breakdown")

    axes[2].scatter(
        risk_df["total_cases"],
        risk_df["pipeline_value"],
        c=risk_df["risk_score"],
        cmap="RdYlGn_r",
        alpha=0.7,
        edgecolors="gray",
        s=60,
    )
    axes[2].set_title("Cases vs Pipeline (color = risk)")
    axes[2].set_xlabel("Open Cases")
    axes[2].set_ylabel("Pipeline Value ($)")

    plt.tight_layout()
    chart_path = OUTPUT_DIR / "risk_assessment_report.png"
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    print(f"[OK] Saved {chart_path}")


# ================================================================
# Pipeline Orchestrator
# ================================================================


async def run_risk_pipeline(client):
    """Run the full async risk assessment pipeline."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"[INFO] Output folder: {OUTPUT_DIR.resolve()}")

    print("\n" + "#" * 60)
    print("  ASYNC CUSTOMER RISK ASSESSMENT PIPELINE")
    print("  Dataverse SDK (async) -> Pandas -> Analysis -> LLM -> Write-back")
    print("#" * 60)

    # Step 1: Extract data concurrently
    accounts, cases, opportunities = await step1_extract(client)

    if accounts.empty:
        print("[WARN] No accounts found -- nothing to analyze.")
        return

    # Step 2: Statistical analysis (pure Python -- synchronous)
    risk_df = step2_analyze(accounts, cases, opportunities)

    # Step 3: LLM-powered risk summarization (synchronous LLM calls)
    # Configure your LLM provider (uncomment one):
    #   Option A: Azure AI Inference
    #     llm = get_llm_client("azure-ai-inference", endpoint="https://...", api_key="...")
    #   Option B: OpenAI
    #     llm = get_llm_client("openai", api_key="sk-...")
    #   Option C: Azure OpenAI (via openai package)
    #     llm = get_llm_client("openai", endpoint="https://...", api_key="...")
    llm = None  # Set to get_llm_client(...) to enable LLM summarization
    risk_df = step3_summarize(risk_df, llm_complete=llm)

    # Step 4: Write results back to Dataverse (async)
    # Uncomment the next line to write back (requires custom columns on account table)
    # await step4_writeback(client, risk_df)
    print("\n[INFO] Step 4 (write-back) is commented out by default.")
    print("  Uncomment step4_writeback() after adding custom columns to account table.")

    # Step 5: Generate summary report + charts (synchronous)
    step5_report(risk_df)


if __name__ == "__main__":
    asyncio.run(main())
