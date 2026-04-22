# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests that every symbol in __all__ is importable from each package namespace,
and that re-exported objects are identical to their originals."""

import unittest


class TestCoreExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.core.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.core as m

        for name in m.__all__:
            self.assertTrue(hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.core")

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.core as m
        from PowerPlatform.Dataverse.core.config import DataverseConfig
        from PowerPlatform.Dataverse.core.errors import (
            DataverseError,
            HttpError,
            MetadataError,
            SQLParseError,
            ValidationError,
        )
        from PowerPlatform.Dataverse.core.log_config import LogConfig

        self.assertIs(m.DataverseConfig, DataverseConfig)
        self.assertIs(m.DataverseError, DataverseError)
        self.assertIs(m.HttpError, HttpError)
        self.assertIs(m.MetadataError, MetadataError)
        self.assertIs(m.SQLParseError, SQLParseError)
        self.assertIs(m.ValidationError, ValidationError)
        self.assertIs(m.LogConfig, LogConfig)


class TestModelsExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.models.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.models as m

        for name in m.__all__:
            self.assertTrue(hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.models")

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.models as m
        from PowerPlatform.Dataverse.models.batch import BatchItemResponse, BatchResult
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption, QueryBuilder, QueryParams
        from PowerPlatform.Dataverse.models.record import Record
        from PowerPlatform.Dataverse.models.relationship import (
            CascadeConfiguration,
            LookupAttributeMetadata,
            ManyToManyRelationshipMetadata,
            OneToManyRelationshipMetadata,
            RelationshipInfo,
        )
        from PowerPlatform.Dataverse.models.table_info import AlternateKeyInfo, ColumnInfo, TableInfo
        from PowerPlatform.Dataverse.models.upsert import UpsertItem

        self.assertIs(m.BatchItemResponse, BatchItemResponse)
        self.assertIs(m.BatchResult, BatchResult)
        self.assertIs(m.ExpandOption, ExpandOption)
        self.assertIs(m.QueryBuilder, QueryBuilder)
        self.assertIs(m.QueryParams, QueryParams)
        self.assertIs(m.Record, Record)
        self.assertIs(m.CascadeConfiguration, CascadeConfiguration)
        self.assertIs(m.LookupAttributeMetadata, LookupAttributeMetadata)
        self.assertIs(m.ManyToManyRelationshipMetadata, ManyToManyRelationshipMetadata)
        self.assertIs(m.OneToManyRelationshipMetadata, OneToManyRelationshipMetadata)
        self.assertIs(m.RelationshipInfo, RelationshipInfo)
        self.assertIs(m.AlternateKeyInfo, AlternateKeyInfo)
        self.assertIs(m.ColumnInfo, ColumnInfo)
        self.assertIs(m.TableInfo, TableInfo)
        self.assertIs(m.UpsertItem, UpsertItem)


class TestOperationsExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.operations.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.operations as m

        for name in m.__all__:
            self.assertTrue(
                hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.operations"
            )

    def test_identity(self):
        """Re-exported objects are the same objects as their source definitions."""
        import PowerPlatform.Dataverse.operations as m
        from PowerPlatform.Dataverse.operations.batch import (
            BatchDataFrameOperations,
            BatchOperations,
            BatchQueryOperations,
            BatchRecordOperations,
            BatchRequest,
            BatchTableOperations,
            ChangeSet,
            ChangeSetRecordOperations,
        )
        from PowerPlatform.Dataverse.operations.dataframe import DataFrameOperations
        from PowerPlatform.Dataverse.operations.files import FileOperations
        from PowerPlatform.Dataverse.operations.query import QueryOperations
        from PowerPlatform.Dataverse.operations.records import RecordOperations
        from PowerPlatform.Dataverse.operations.tables import TableOperations

        self.assertIs(m.BatchDataFrameOperations, BatchDataFrameOperations)
        self.assertIs(m.BatchOperations, BatchOperations)
        self.assertIs(m.BatchQueryOperations, BatchQueryOperations)
        self.assertIs(m.BatchRecordOperations, BatchRecordOperations)
        self.assertIs(m.BatchRequest, BatchRequest)
        self.assertIs(m.BatchTableOperations, BatchTableOperations)
        self.assertIs(m.ChangeSet, ChangeSet)
        self.assertIs(m.ChangeSetRecordOperations, ChangeSetRecordOperations)
        self.assertIs(m.DataFrameOperations, DataFrameOperations)
        self.assertIs(m.FileOperations, FileOperations)
        self.assertIs(m.QueryOperations, QueryOperations)
        self.assertIs(m.RecordOperations, RecordOperations)
        self.assertIs(m.TableOperations, TableOperations)


if __name__ == "__main__":
    unittest.main()
