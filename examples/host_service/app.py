"""FastAPI façade that exposes DataverseClient operations over HTTP."""

from __future__ import annotations

import os
import base64
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional

from azure.identity import CertificateCredential, ClientSecretCredential
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import DataverseError, HttpError

_BASE_REQUIRED_ENV_VARS = (
    "DATAVERSE_BASE_URL",
    "DATAVERSE_TENANT_ID",
    "DATAVERSE_CLIENT_ID",
)


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable '{name}' is required.")
    return value


@lru_cache(maxsize=1)
def _dataverse_client() -> DataverseClient:
    base_url = _require_env("DATAVERSE_BASE_URL")
    credential = _build_credential()
    return DataverseClient(base_url=base_url, credential=credential)


def _build_credential():
    tenant_id = _require_env("DATAVERSE_TENANT_ID")
    client_id = _require_env("DATAVERSE_CLIENT_ID")

    client_secret = os.getenv("DATAVERSE_CLIENT_SECRET", "").strip()
    certificate_path = _resolve_certificate_path()
    certificate_password = os.getenv("DATAVERSE_CLIENT_CERT_PASSWORD", None)

    if client_secret:
        return ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

    if certificate_path:
        return CertificateCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            certificate_path=certificate_path,
            password=certificate_password if certificate_password else None,
        )

    raise RuntimeError(
        "Supply DATAVERSE_CLIENT_SECRET or DATAVERSE_CLIENT_CERT_PATH (plus optional DATAVERSE_CLIENT_CERT_PASSWORD)."
    )


def _resolve_certificate_path() -> Optional[str]:
    encoded = os.getenv("DATAVERSE_CLIENT_CERT_BASE64", "").strip()
    direct_path = os.getenv("DATAVERSE_CLIENT_CERT_PATH", "").strip()

    if encoded:
        target_path = direct_path or os.getenv("DATAVERSE_CLIENT_CERT_TMP_PATH", "/tmp/dataverse_cert.pfx")
        data = base64.b64decode(encoded)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as cert_file:
            cert_file.write(data)
        return target_path

    if direct_path:
        if not os.path.exists(direct_path):
            raise RuntimeError(f"Certificate file '{direct_path}' does not exist.")
        return direct_path

    return None


def _split_csv(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    entries = [part.strip() for part in value.split(",") if part.strip()]
    return entries or None


def _flatten_batches(batches: Iterable[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for batch in batches:
        records.extend(batch)
    return records


def _translate_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HttpError):
        return HTTPException(status_code=exc.status_code or 502, detail=exc.to_dict())
    if isinstance(exc, DataverseError):
        return HTTPException(status_code=400, detail=exc.to_dict())
    return HTTPException(status_code=500, detail={"message": str(exc)})


class RecordPayload(BaseModel):
    data: Dict[str, Any] = Field(..., description="Attribute logical names mapped to values.")


class SqlQuery(BaseModel):
    query: str = Field(..., min_length=1, description="Supported Dataverse SQL SELECT statement.")


app = FastAPI(title="Dataverse Python SDK Host", version="1.0.0")


@app.middleware("http")
async def _require_api_key(request: Request, call_next):
    expected_key = os.getenv("CONNECTOR_API_KEY", "").strip()
    if expected_key and request.headers.get("x-api-key") != expected_key:
        raise HTTPException(status_code=401, detail={"message": "Invalid API key"})
    return await call_next(request)


@app.on_event("startup")
def _startup_probe() -> None:
    """Fail fast if required environment variables are absent."""
    for env_name in _BASE_REQUIRED_ENV_VARS:
        _require_env(env_name)
    if not (
        os.getenv("DATAVERSE_CLIENT_SECRET")
        or os.getenv("DATAVERSE_CLIENT_CERT_PATH")
        or os.getenv("DATAVERSE_CLIENT_CERT_BASE64")
    ):
        raise RuntimeError(
            "Set DATAVERSE_CLIENT_SECRET or DATAVERSE_CLIENT_CERT_PATH / DATAVERSE_CLIENT_CERT_BASE64 before starting the container."
        )
    # Warm the Dataverse client so container start failures surface immediately.
    _dataverse_client()


@app.get("/health")
def health() -> Dict[str, Any]:
    base_url = _require_env("DATAVERSE_BASE_URL")
    return {
        "status": "ok",
        "base_url": base_url,
        "revision": os.getenv("CONTAINER_APP_REVISION"),
    }


@app.get("/records/{table_schema_name}")
def query_records(
    table_schema_name: str,
    filter: Optional[str] = Query(default=None, description="OData filter string."),
    select: Optional[str] = Query(default=None, description="Comma-separated list of columns."),
    orderby: Optional[str] = Query(default=None, description="Comma-separated order by clauses."),
    top: Optional[int] = Query(default=None, gt=0, le=5000),
    page_size: Optional[int] = Query(default=None, gt=0, le=5000),
) -> Dict[str, Any]:
    client = _dataverse_client()
    try:
        batches = client.get(
            table_schema_name,
            select=_split_csv(select),
            filter=filter,
            orderby=_split_csv(orderby),
            top=top,
            page_size=page_size,
        )
        records = _flatten_batches(batches)
        return {"records": records, "count": len(records)}
    except Exception as exc:
        raise _translate_error(exc) from exc


@app.get("/records/{table_schema_name}/{record_id}")
def get_record(table_schema_name: str, record_id: str, select: Optional[str] = None) -> Dict[str, Any]:
    client = _dataverse_client()
    try:
        record = client.get(table_schema_name, record_id=record_id, select=_split_csv(select))
        return {"record": record}
    except Exception as exc:
        raise _translate_error(exc) from exc


@app.post("/records/{table_schema_name}")
def create_record(table_schema_name: str, payload: RecordPayload) -> Dict[str, Any]:
    client = _dataverse_client()
    try:
        record_id = client.create(table_schema_name, payload.data)
        return {"id": record_id}
    except Exception as exc:
        raise _translate_error(exc) from exc


@app.patch("/records/{table_schema_name}/{record_id}")
def update_record(table_schema_name: str, record_id: str, payload: RecordPayload) -> Dict[str, Any]:
    client = _dataverse_client()
    try:
        client.update(table_schema_name, record_id, payload.data)
        return {"id": record_id, "status": "updated"}
    except Exception as exc:
        raise _translate_error(exc) from exc


@app.post("/sql")
def run_sql(payload: SqlQuery) -> Dict[str, Any]:
    client = _dataverse_client()
    try:
        rows = client.query_sql(payload.query)
        return {"records": rows, "count": len(rows)}
    except Exception as exc:
        raise _translate_error(exc) from exc
