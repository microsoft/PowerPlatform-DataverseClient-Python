"""Temporary script to compare module __all__ vs package __init__.py re-exports."""
import importlib

packages = {
    "models": [
        "PowerPlatform.Dataverse.models.batch",
        "PowerPlatform.Dataverse.models.fetchxml_query",
        "PowerPlatform.Dataverse.models.filters",
        "PowerPlatform.Dataverse.models.labels",
        "PowerPlatform.Dataverse.models.protocol",
        "PowerPlatform.Dataverse.models.query_builder",
        "PowerPlatform.Dataverse.models.record",
        "PowerPlatform.Dataverse.models.relationship",
        "PowerPlatform.Dataverse.models.table_info",
        "PowerPlatform.Dataverse.models.upsert",
    ],
    "core": [
        "PowerPlatform.Dataverse.core.config",
        "PowerPlatform.Dataverse.core.errors",
        "PowerPlatform.Dataverse.core.log_config",
    ],
    "operations": [
        "PowerPlatform.Dataverse.operations.batch",
        "PowerPlatform.Dataverse.operations.dataframe",
        "PowerPlatform.Dataverse.operations.files",
        "PowerPlatform.Dataverse.operations.query",
        "PowerPlatform.Dataverse.operations.records",
        "PowerPlatform.Dataverse.operations.tables",
    ],
}

for pkg_name, modules in packages.items():
    pkg = importlib.import_module("PowerPlatform.Dataverse." + pkg_name)
    pkg_all = set(pkg.__all__)
    all_module_exports = set()

    for mod_name in modules:
        mod = importlib.import_module(mod_name)
        mod_all = set(getattr(mod, "__all__", []))
        all_module_exports |= mod_all
        missing = mod_all - pkg_all
        if missing:
            short = mod_name.split(".")[-1]
            print("MISSING from " + pkg_name + "/__init__.py (in " + short + "): " + str(sorted(missing)))

    extra = pkg_all - all_module_exports
    if extra:
        print("EXTRA in " + pkg_name + "/__init__.py: " + str(sorted(extra)))

    if not (all_module_exports - pkg_all) and not extra:
        print(pkg_name + ": All exports match perfectly")
