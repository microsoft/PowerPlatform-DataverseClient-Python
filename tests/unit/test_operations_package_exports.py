# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse import operations
from PowerPlatform.Dataverse.operations import (
    BatchDataFrameOperations,
    BatchOperations,
    BatchQueryOperations,
    BatchRecordOperations,
    BatchRequest,
    BatchTableOperations,
    ChangeSet,
    ChangeSetRecordOperations,
    DataFrameOperations,
    FileOperations,
    QueryOperations,
    RecordOperations,
    TableOperations,
)


class TestOperationsPackageExports(unittest.TestCase):
    """Tests for package-level exports in PowerPlatform.Dataverse.operations."""

    def test_package_level_imports_work(self):
        """Expected operation namespace classes are importable from package root."""
        self.assertIs(operations.RecordOperations, RecordOperations)
        self.assertIs(operations.QueryOperations, QueryOperations)
        self.assertIs(operations.TableOperations, TableOperations)
        self.assertIs(operations.FileOperations, FileOperations)
        self.assertIs(operations.DataFrameOperations, DataFrameOperations)

        self.assertIs(operations.BatchOperations, BatchOperations)
        self.assertIs(operations.BatchRecordOperations, BatchRecordOperations)
        self.assertIs(operations.BatchQueryOperations, BatchQueryOperations)
        self.assertIs(operations.BatchTableOperations, BatchTableOperations)
        self.assertIs(operations.BatchDataFrameOperations, BatchDataFrameOperations)
        self.assertIs(operations.BatchRequest, BatchRequest)
        self.assertIs(operations.ChangeSet, ChangeSet)
        self.assertIs(operations.ChangeSetRecordOperations, ChangeSetRecordOperations)

    def test_all_exports_include_expected_symbols(self):
        """__all__ should expose the package-level operation symbols."""
        expected_exports = {
            "BatchDataFrameOperations",
            "BatchOperations",
            "BatchQueryOperations",
            "BatchRecordOperations",
            "BatchRequest",
            "BatchTableOperations",
            "ChangeSet",
            "ChangeSetRecordOperations",
            "DataFrameOperations",
            "FileOperations",
            "QueryOperations",
            "RecordOperations",
            "TableOperations",
        }
        self.assertEqual(set(operations.__all__), expected_exports)
