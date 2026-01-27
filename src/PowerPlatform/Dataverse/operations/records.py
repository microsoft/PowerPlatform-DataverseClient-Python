# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Record CRUD operations namespace."""

from __future__ import annotations

from typing import Any, Dict, Optional, Union, List, TYPE_CHECKING

from ..core.results import OperationResult, RequestTelemetryData

if TYPE_CHECKING:
    from ..client import DataverseClient


class RecordOperations:
    """
    Record CRUD operations with method overloading for single/bulk operations.

    Accessed via ``client.records``. Uses the same method name for both single
    and bulk operations, detecting the operation type from parameter types.

    Example:
        Single record operations::

            # Create single record
            ids = client.records.create("account", {"name": "Contoso"})

            # Update single record
            client.records.update("account", record_id, {"name": "Updated"})

            # Delete single record
            client.records.delete("account", record_id)

            # Get single record
            record = client.records.get("account", record_id)

        Bulk record operations (same methods, different signatures)::

            # Create multiple records
            ids = client.records.create("account", [
                {"name": "Contoso"},
                {"name": "Fabrikam"}
            ])

            # Update multiple records (broadcast same changes)
            client.records.update("account", [id1, id2], {"status": 1})

            # Update multiple records (paired changes)
            client.records.update("account", [id1, id2], [changes1, changes2])

            # Delete multiple records
            client.records.delete("account", [id1, id2, id3])

        With telemetry access::

            response = client.records.create("account", data).with_response_details()
            print(response.telemetry["service_request_id"])
    """

    def __init__(self, client: "DataverseClient") -> None:
        """
        Initialize RecordOperations.

        :param client: Parent DataverseClient instance.
        :type client: DataverseClient
        """
        self._client = client

    def create(
        self,
        table: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> OperationResult[List[str]]:
        """
        Create one or more records.

        Automatically detects single vs bulk based on input type:
        - dict: Creates single record
        - list[dict]: Creates multiple records using batch API

        :param table: Table schema name (e.g., "account", "new_MyTable").
        :type table: str
        :param data: Single record dict or list of record dicts.
        :type data: dict or list[dict]
        :return: OperationResult containing list of created record GUIDs.
        :rtype: OperationResult[List[str]]

        :raises TypeError: If ``data`` is not a dict or list[dict], or if the internal
            client returns an unexpected type.

        Example:
            Single record::

                ids = client.records.create("account", {"name": "Contoso"})
                print(ids[0])  # GUID string

            Multiple records::

                ids = client.records.create("account", [
                    {"name": "Contoso"},
                    {"name": "Fabrikam"}
                ])
                for id in ids:
                    print(id)

            Access telemetry data::

                response = client.records.create("account", {"name": "Test"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(table)
            if isinstance(data, dict):
                rid, metadata = od._create(entity_set, table, data)
                if not isinstance(rid, str):
                    raise TypeError("_create (single) did not return GUID string")
                return OperationResult([rid], metadata)
            if isinstance(data, list):
                ids, metadata = od._create_multiple(entity_set, table, data)
                if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                    raise TypeError("_create (multi) did not return list[str]")
                return OperationResult(ids, metadata)
        raise TypeError("data must be dict or list[dict]")

    def update(
        self,
        table: str,
        ids: Union[str, List[str]],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> OperationResult[None]:
        """
        Update one or more records.

        Supports three patterns:
        1. Single: update(table, "guid", {changes})
        2. Broadcast: update(table, [id1, id2], {same_changes})
        3. Paired: update(table, [id1, id2], [changes1, changes2])

        :param table: Table schema name.
        :type table: str
        :param ids: Single GUID or list of GUIDs.
        :type ids: str or list[str]
        :param changes: Changes dict or list of changes dicts.
        :type changes: dict or list[dict]
        :return: OperationResult containing None.
        :rtype: OperationResult[None]

        :raises TypeError: If ``ids`` is not str or list[str], or if ``changes`` type
            doesn't match usage pattern.

        Example:
            Single record update::

                client.records.update("account", account_id, {"telephone1": "555-0100"})

            Broadcast same changes to multiple records::

                client.records.update("account", [id1, id2, id3], {"statecode": 1})

            Access telemetry data::

                response = client.records.update("account", id, {"name": "New"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            # Unwrap OperationResult if passed directly from create()
            if isinstance(ids, OperationResult):
                ids = ids.value
            if isinstance(ids, str):
                if not isinstance(changes, dict):
                    raise TypeError("For single id, changes must be a dict")
                _, metadata = od._update(table, ids, changes)
                return OperationResult(None, metadata)
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            _, metadata = od._update_by_ids(table, ids, changes)
            return OperationResult(None, metadata)

    def delete(
        self,
        table: str,
        ids: Union[str, List[str]],
        use_bulk_delete: bool = True,
    ) -> OperationResult[Optional[str]]:
        """
        Delete one or more records.

        :param table: Table schema name.
        :type table: str
        :param ids: Single GUID or list of GUIDs to delete.
        :type ids: str or list[str]
        :param use_bulk_delete: For bulk deletes, use async BulkDelete job.
        :type use_bulk_delete: bool
        :return: OperationResult containing bulk delete job ID (if applicable) or None.
        :rtype: OperationResult[Optional[str]]

        :raises TypeError: If ``ids`` is not str or list[str].

        Example:
            Delete a single record::

                client.records.delete("account", account_id)

            Delete multiple records and get job ID::

                result = client.records.delete("account", [id1, id2, id3])
                job_id = result.value  # Access the job ID directly

            Access telemetry data::

                response = client.records.delete("account", id).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            # Unwrap OperationResult if passed directly from create()
            if isinstance(ids, OperationResult):
                ids = ids.value
            if isinstance(ids, str):
                _, metadata = od._delete(table, ids)
                return OperationResult(None, metadata)
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            if not ids:
                return OperationResult(None, RequestTelemetryData())
            if not all(isinstance(rid, str) for rid in ids):
                raise TypeError("ids must contain string GUIDs")
            if use_bulk_delete:
                job_id, metadata = od._delete_multiple(table, ids)
                return OperationResult(job_id, metadata)
            # Sequential deletes - capture metadata from the last delete
            metadata = RequestTelemetryData()
            for rid in ids:
                _, metadata = od._delete(table, rid)
            return OperationResult(None, metadata)

    def get(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
        expand: Optional[List[str]] = None,
    ) -> OperationResult[Dict[str, Any]]:
        """
        Get a single record by ID.

        For querying multiple records, use ``client.query.get()``.

        :param table: Table schema name.
        :type table: str
        :param record_id: Record GUID.
        :type record_id: str
        :param select: Optional list of columns to retrieve.
        :type select: list[str] or None
        :param expand: Optional navigation properties to expand.
        :type expand: list[str] or None
        :return: OperationResult containing the record dict.
        :rtype: OperationResult[Dict[str, Any]]

        :raises TypeError: If ``record_id`` is not a string.

        Example:
            Fetch a single record::

                record = client.records.get("account", account_id, select=["name", "telephone1"])
                print(record["name"])  # Works via __getitem__

            Fetch single record with telemetry::

                response = client.records.get("account", account_id).with_response_details()
                print(f"Record: {response.result['name']}")
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        if not isinstance(record_id, str):
            raise TypeError("record_id must be str")
        with self._client._scoped_odata() as od:
            record, metadata = od._get(
                table,
                record_id,
                select=select,
            )
            return OperationResult(record, metadata)

    def upsert(
        self,
        table: str,
        data: Dict[str, Any],
        *,
        alternate_key: Optional[Dict[str, Any]] = None,
    ) -> OperationResult[Dict[str, Any]]:
        """
        Insert or update record based on alternate key.

        .. note::
            This method is a placeholder for future implementation (Priority 9).
            Currently raises NotImplementedError.

        :param table: Table schema name.
        :type table: str
        :param data: Record data.
        :type data: dict
        :param alternate_key: Alternate key for matching existing records.
        :type alternate_key: dict or None
        :raises NotImplementedError: Upsert is planned for a future release.
        """
        raise NotImplementedError("Upsert operation is planned for a future release.")


__all__ = ["RecordOperations"]
