# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for async package-level re-exports.

Each async package (``PowerPlatform.Dataverse.aio``,
``aio.operations``, ``aio.models``) re-exports its public symbols
and declares them in ``__all__``. This gives users short, stable
import paths (``from PowerPlatform.Dataverse.aio import AsyncDataverseClient``)
that survive internal module reorganization.

These tests verify:
1. ``__all__`` matches the expected list exactly (catches accidental drift).
2. Every name in ``__all__`` is importable from the package namespace.
3. Each re-export is the same object as its source definition.
"""

import unittest

AIO_EXPECTED = [
    "AsyncDataverseClient",
]

AIO_OPERATIONS_EXPECTED = [
    "AsyncBatchOperations",
    "AsyncBatchRequest",
    "AsyncChangeSet",
    "AsyncDataFrameOperations",
    "AsyncFileOperations",
    "AsyncQueryOperations",
    "AsyncRecordOperations",
    "AsyncTableOperations",
]

AIO_MODELS_EXPECTED = [
    "AsyncFetchXmlQuery",
    "AsyncQueryBuilder",
]


class TestAioTopLevelExports(unittest.TestCase):
    """Verify top-level PowerPlatform.Dataverse.aio package exports."""

    def test_all_matches_expected(self):
        """``__all__`` matches the expected list exactly."""
        import PowerPlatform.Dataverse.aio as m

        self.assertEqual(sorted(m.__all__), sorted(AIO_EXPECTED))

    def test_expected_symbols_importable(self):
        """Every expected public symbol is reachable from the package namespace."""
        import PowerPlatform.Dataverse.aio as m

        for name in AIO_EXPECTED:
            self.assertTrue(hasattr(m, name), f"{name!r} not importable from PowerPlatform.Dataverse.aio")

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.aio as m
        from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

        self.assertIs(m.AsyncDataverseClient, AsyncDataverseClient)


class TestAioOperationsExports(unittest.TestCase):
    """Verify package-level imports for PowerPlatform.Dataverse.aio.operations."""

    def test_all_matches_expected(self):
        """``__all__`` matches the expected list exactly."""
        import PowerPlatform.Dataverse.aio.operations as m

        self.assertEqual(sorted(m.__all__), sorted(AIO_OPERATIONS_EXPECTED))

    def test_expected_symbols_importable(self):
        """Every expected public symbol is reachable from the package namespace."""
        import PowerPlatform.Dataverse.aio.operations as m

        for name in AIO_OPERATIONS_EXPECTED:
            self.assertTrue(hasattr(m, name), f"{name!r} not importable from PowerPlatform.Dataverse.aio.operations")

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.aio.operations as m
        from PowerPlatform.Dataverse.aio.operations.async_batch import (
            AsyncBatchOperations,
            AsyncBatchRequest,
            AsyncChangeSet,
        )
        from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations
        from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations
        from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryOperations
        from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
        from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations

        self.assertIs(m.AsyncBatchOperations, AsyncBatchOperations)
        self.assertIs(m.AsyncBatchRequest, AsyncBatchRequest)
        self.assertIs(m.AsyncChangeSet, AsyncChangeSet)
        self.assertIs(m.AsyncDataFrameOperations, AsyncDataFrameOperations)
        self.assertIs(m.AsyncFileOperations, AsyncFileOperations)
        self.assertIs(m.AsyncQueryOperations, AsyncQueryOperations)
        self.assertIs(m.AsyncRecordOperations, AsyncRecordOperations)
        self.assertIs(m.AsyncTableOperations, AsyncTableOperations)


class TestAioModelsExports(unittest.TestCase):
    """Verify package-level imports for PowerPlatform.Dataverse.aio.models."""

    def test_all_matches_expected(self):
        """``__all__`` matches the expected list exactly."""
        import PowerPlatform.Dataverse.aio.models as m

        self.assertEqual(sorted(m.__all__), sorted(AIO_MODELS_EXPECTED))

    def test_expected_symbols_importable(self):
        """Every expected public symbol is reachable from the package namespace."""
        import PowerPlatform.Dataverse.aio.models as m

        for name in AIO_MODELS_EXPECTED:
            self.assertTrue(hasattr(m, name), f"{name!r} not importable from PowerPlatform.Dataverse.aio.models")

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.aio.models as m
        from PowerPlatform.Dataverse.aio.models.async_fetchxml_query import AsyncFetchXmlQuery
        from PowerPlatform.Dataverse.aio.models.async_query_builder import AsyncQueryBuilder

        self.assertIs(m.AsyncFetchXmlQuery, AsyncFetchXmlQuery)
        self.assertIs(m.AsyncQueryBuilder, AsyncQueryBuilder)


if __name__ == "__main__":
    unittest.main()
