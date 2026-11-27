# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import sys
import unittest
from unittest.mock import MagicMock, patch

from azure.core.credentials import TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_batch import AsyncBatchRequest
from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
from PowerPlatform.Dataverse.models.record import QueryResult
from PowerPlatform.Dataverse.operations.batch import BatchDataFrameOperations, BatchRequest
from PowerPlatform.Dataverse.operations.dataframe import DataFrameOperations


class TestOptionalPandasWithoutPandas(unittest.TestCase):
    """Verify behavior when pandas is not installed."""

    def setUp(self):
        self.mock_cred = MagicMock(spec=TokenCredential)
        self.async_mock_cred = MagicMock(spec=AsyncTokenCredential)

    def test_client_initialization_without_pandas(self):
        """DataverseClient initializes and exposes non-dataframe namespaces without pandas."""
        with patch.dict(sys.modules, {"pandas": None}):
            client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
            self.assertIsNotNone(client.records)
            self.assertIsNotNone(client.query)
            self.assertIsNotNone(client.tables)
            self.assertIsNotNone(client.files)
            self.assertIsNotNone(client.batch)

    def test_batch_creation_without_pandas(self):
        """BatchRequest initializes and exposes non-dataframe namespaces without pandas."""
        with patch.dict(sys.modules, {"pandas": None}):
            client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
            batch = client.batch.new()
            self.assertIsInstance(batch, BatchRequest)
            self.assertIsNotNone(batch.records)
            self.assertIsNotNone(batch.tables)
            self.assertIsNotNone(batch.query)

    def test_client_dataframe_access_raises_import_error(self):
        """Accessing client.dataframe without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
            with self.assertRaises(ImportError) as ctx:
                _ = client.dataframe
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))

    def test_batch_dataframe_access_raises_import_error(self):
        """Accessing batch.dataframe without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
            batch = client.batch.new()
            with self.assertRaises(ImportError) as ctx:
                _ = batch.dataframe
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))

    def test_async_client_initialization_without_pandas(self):
        """AsyncDataverseClient initializes without pandas."""
        with patch.dict(sys.modules, {"pandas": None}):
            async_client = AsyncDataverseClient("https://example.crm.dynamics.com", self.async_mock_cred)
            self.assertIsNotNone(async_client.records)
            self.assertIsNotNone(async_client.query)
            self.assertIsNotNone(async_client.tables)
            self.assertIsNotNone(async_client.files)
            self.assertIsNotNone(async_client.batch)

    def test_async_client_dataframe_access_raises_import_error(self):
        """Accessing async_client.dataframe without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            async_client = AsyncDataverseClient("https://example.crm.dynamics.com", self.async_mock_cred)
            with self.assertRaises(ImportError) as ctx:
                _ = async_client.dataframe
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))

    def test_async_batch_dataframe_access_raises_import_error(self):
        """Accessing async_batch.dataframe without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            async_client = AsyncDataverseClient("https://example.crm.dynamics.com", self.async_mock_cred)
            async_batch = async_client.batch.new()
            self.assertIsInstance(async_batch, AsyncBatchRequest)
            with self.assertRaises(ImportError) as ctx:
                _ = async_batch.dataframe
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))

    def test_query_result_to_dataframe_raises_import_error(self):
        """QueryResult.to_dataframe() without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            qr = QueryResult([])
            with self.assertRaises(ImportError) as ctx:
                qr.to_dataframe()
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))

    def test_query_builder_to_dataframe_raises_import_error(self):
        """QueryBuilder.to_dataframe() without pandas raises informative ImportError."""
        with patch.dict(sys.modules, {"pandas": None}):
            qb = QueryBuilder("account")
            with self.assertRaises(ImportError) as ctx:
                qb.to_dataframe()
            self.assertIn("PowerPlatform-Dataverse-Client[dataframe]", str(ctx.exception))


class TestOptionalPandasWithPandas(unittest.TestCase):
    """Verify standard property behavior when pandas is present."""

    def setUp(self):
        self.mock_cred = MagicMock(spec=TokenCredential)
        self.async_mock_cred = MagicMock(spec=AsyncTokenCredential)

    def test_client_dataframe_property(self):
        """client.dataframe returns DataFrameOperations and allows setter assignment."""
        client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
        df_ops = client.dataframe
        self.assertIsInstance(df_ops, DataFrameOperations)
        self.assertIs(client.dataframe, df_ops)

        mock_ops = MagicMock(spec=DataFrameOperations)
        client.dataframe = mock_ops
        self.assertIs(client.dataframe, mock_ops)

    def test_async_client_dataframe_property(self):
        """async_client.dataframe returns AsyncDataFrameOperations and allows setter assignment."""
        async_client = AsyncDataverseClient("https://example.crm.dynamics.com", self.async_mock_cred)
        df_ops = async_client.dataframe
        self.assertIsInstance(df_ops, AsyncDataFrameOperations)
        self.assertIs(async_client.dataframe, df_ops)

        mock_ops = MagicMock(spec=AsyncDataFrameOperations)
        async_client.dataframe = mock_ops
        self.assertIs(async_client.dataframe, mock_ops)

    def test_batch_dataframe_property(self):
        """batch.dataframe returns BatchDataFrameOperations and allows setter assignment."""
        client = DataverseClient("https://example.crm.dynamics.com", self.mock_cred)
        batch = client.batch.new()
        batch_df_ops = batch.dataframe
        self.assertIsInstance(batch_df_ops, BatchDataFrameOperations)
        self.assertIs(batch.dataframe, batch_df_ops)

        mock_ops = MagicMock(spec=BatchDataFrameOperations)
        batch.dataframe = mock_ops
        self.assertIs(batch.dataframe, mock_ops)

    def test_async_batch_dataframe_property(self):
        """async_batch.dataframe returns BatchDataFrameOperations and allows setter assignment."""
        async_client = AsyncDataverseClient("https://example.crm.dynamics.com", self.async_mock_cred)
        async_batch = async_client.batch.new()
        batch_df_ops = async_batch.dataframe
        self.assertIsInstance(batch_df_ops, BatchDataFrameOperations)
        self.assertIs(async_batch.dataframe, batch_df_ops)

        mock_ops = MagicMock(spec=BatchDataFrameOperations)
        async_batch.dataframe = mock_ops
        self.assertIs(async_batch.dataframe, mock_ops)
