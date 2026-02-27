"""
Telemetry Demo - Demonstrates OpenTelemetry integration with Dataverse SDK.

This script shows telemetry flowing through:
1. Custom hooks (always -- no extra dependencies)
2. Console span/metric exporters (requires opentelemetry-sdk)
3. Jaeger UI via OTLP (optional, if running locally via Docker)

To run with hooks only (no OTel dependency):
    python examples/telemetry_demo.py

To run with full OTel:
    pip install "PowerPlatform-Dataverse-Client[telemetry]"
    python examples/telemetry_demo.py

To run Jaeger locally:
    docker run -d --name jaeger -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/all-in-one:latest

Then open http://localhost:16686 to see traces.

Usage:
    python examples/telemetry_demo.py
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# =============================================================================
# OpenTelemetry Setup (optional -- demo works with hooks alone)
# =============================================================================

OTEL_CONFIGURED = False

try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    resource = Resource.create({"service.name": "dataverse-telemetry-demo"})

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # Try OTLP exporter for Jaeger
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True))
        )
        print("OTLP exporter configured - view traces at http://localhost:16686")
    except ImportError:
        pass

    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter(), export_interval_millis=5000)
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))

    OTEL_CONFIGURED = True
    print("OpenTelemetry configured with Console exporter")

except ImportError:
    print("OpenTelemetry not installed -- running with hooks only")
    print("  Install with: pip install opentelemetry-sdk opentelemetry-api")

print("-" * 60)

# =============================================================================
# Dataverse SDK Setup
# =============================================================================

import os

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.telemetry import TelemetryConfig, TelemetryHook
from azure.identity import InteractiveBrowserCredential

# Org details -- set via environment variables
ORG_URL = os.environ.get("DATAVERSE_URL", "https://YOUR_ORG.crm.dynamics.com")
TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")


class DemoTelemetryHook(TelemetryHook):
    """Custom hook that prints request/response info to the console."""

    def on_request_start(self, ctx):
        print(f"\n>>> Starting: {ctx.operation} [{ctx.method}]")
        if ctx.table_name:
            print(f"    Table: {ctx.table_name}")

    def on_request_end(self, request, response):
        status = "[OK]" if response.status_code < 400 else "[ERR]"
        print(f"<<< {status} {request.operation} - {response.status_code} in {response.duration_ms:.1f}ms")
        if response.service_request_id:
            print(f"    Service Request ID: {response.service_request_id}")

    def on_request_error(self, request, error):
        print(f"!!! Error in {request.operation}: {error}")


def main():
    print("\n" + "=" * 60)
    print("DATAVERSE TELEMETRY DEMO")
    print("=" * 60)

    config = DataverseConfig(
        telemetry=TelemetryConfig(
            enable_tracing=OTEL_CONFIGURED,
            enable_metrics=OTEL_CONFIGURED,
            enable_logging=True,
            log_level="DEBUG",
            hooks=[DemoTelemetryHook()],
        )
    )

    print(f"\nConnecting to: {ORG_URL}")
    print("(Browser will open for authentication)\n")

    credential = InteractiveBrowserCredential(tenant_id=TENANT_ID)
    client = DataverseClient(ORG_URL, credential, config=config)

    # ---- Operation 1: Query accounts ----
    print("\n" + "-" * 60)
    print("OPERATION 1: Query accounts (top 3)")
    print("-" * 60)

    for page in client.records.get("account", select=["name", "accountid"], top=3):
        print(f"\nFound {len(page)} accounts:")
        for record in page:
            print(f"  - {record.get('name', 'N/A')} ({record.get('accountid', 'N/A')[:8]}...)")

    # ---- Operation 2: SQL query ----
    print("\n" + "-" * 60)
    print("OPERATION 2: SQL query for contacts")
    print("-" * 60)

    rows = client.query.sql("SELECT TOP 3 fullname, emailaddress1 FROM contact ORDER BY fullname")
    print(f"\nFound {len(rows)} contacts:")
    for row in rows:
        print(f"  - {row.get('fullname', 'N/A')} <{row.get('emailaddress1', 'N/A')}>")

    # ---- Operation 3: Table metadata ----
    print("\n" + "-" * 60)
    print("OPERATION 3: Get table metadata")
    print("-" * 60)

    info = client.tables.get("account")
    if info:
        print(f"\nTable: {info.get('table_schema_name')}")
        print(f"  Logical: {info.get('table_logical_name')}")
        print(f"  Entity Set: {info.get('entity_set_name')}")

    # ---- Operation 4: Ad-hoc telemetry capture (no hooks needed) ----
    print("\n" + "-" * 60)
    print("OPERATION 4: Ad-hoc capture_telemetry()")
    print("-" * 60)

    # Create a second client WITHOUT any telemetry config
    plain_client = DataverseClient(ORG_URL, credential)

    with plain_client.capture_telemetry() as t:
        plain_client.tables.get("account")

    print(f"\nCaptured {len(t.requests)} request(s) (no hooks configured!):")
    for req in t.requests:
        print(f"  {req.operation} [{req.method}] {req.status_code} in {req.duration_ms:.0f}ms")
        print(f"    service_request_id: {req.service_request_id}")

    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print("\nCheck the console output above for:")
    print("  - Hook output (>>> / <<< lines)")
    print("  - capture_telemetry() output (no hooks needed)")
    if OTEL_CONFIGURED:
        print("  - Span traces (name, attributes, duration)")
        print("  - Metrics (request counts, durations)")
        print("\nIf Jaeger is running, view traces at: http://localhost:16686")
    print()


if __name__ == "__main__":
    main()
