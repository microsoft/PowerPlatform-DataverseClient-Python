# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests that every symbol in __all__ is importable from each package namespace."""

import unittest


class TestCoreExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.core.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.core as m

        for name in m.__all__:
            self.assertTrue(hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.core")


class TestModelsExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.models.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.models as m

        for name in m.__all__:
            self.assertTrue(hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.models")


class TestOperationsExports(unittest.TestCase):
    """Every name in PowerPlatform.Dataverse.operations.__all__ must be importable."""

    def test_all_symbols_importable(self):
        import PowerPlatform.Dataverse.operations as m

        for name in m.__all__:
            self.assertTrue(
                hasattr(m, name), f"{name!r} is in __all__ but missing from PowerPlatform.Dataverse.operations"
            )


if __name__ == "__main__":
    unittest.main()
