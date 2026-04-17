# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""CLI entry point for the Dataverse entity class generator.

Usage::

    python -m PowerPlatform.Dataverse.generator \\
        --url https://yourorg.crm.dynamics.com \\
        --entities account contact new_mycustomtable \\
        --output Types/

When ``--entities`` is omitted, **all** non-private tables in the org are
generated.  This can be slow; filtering to the tables you actually use is
recommended.

Authentication uses :class:`~azure.identity.InteractiveBrowserCredential` by
default (opens a browser window).  Set ``--auth device`` to use device-code
flow instead (useful in headless / SSH environments).
"""

from __future__ import annotations

import argparse
import sys


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m PowerPlatform.Dataverse.generator",
        description="Generate typed Python entity classes from a Dataverse environment.",
    )
    p.add_argument(
        "--url", "-u",
        required=True,
        metavar="ORG_URL",
        help="Dataverse environment URL, e.g. https://yourorg.crm.dynamics.com",
    )
    p.add_argument(
        "--entities", "-e",
        nargs="*",
        metavar="TABLE",
        default=None,
        help=(
            "Logical names of tables to generate (e.g. account contact new_mytable). "
            "Omit to generate all non-private tables."
        ),
    )
    p.add_argument(
        "--output", "-o",
        default="Types",
        metavar="DIR",
        help="Output directory for generated .py files (default: Types/).",
    )
    p.add_argument(
        "--auth",
        choices=["browser", "device"],
        default="browser",
        help="Authentication method (default: browser).",
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress progress output.",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential
    except ImportError:
        print(
            "ERROR: azure-identity is required.  Install it with:\n"
            "  pip install azure-identity",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        from ..client import DataverseClient
        from . import generate
    except ImportError as exc:
        print(f"ERROR: Could not import DataverseClient: {exc}", file=sys.stderr)
        sys.exit(1)

    # Credential
    if args.auth == "device":
        credential = DeviceCodeCredential()
    else:
        credential = InteractiveBrowserCredential()

    client = DataverseClient(args.url, credential)

    written = generate(
        client,
        entities=args.entities or None,
        output_dir=args.output,
        verbose=not args.quiet,
    )

    if not args.quiet:
        print("\nGenerated files:")
        for path in written:
            print(f"  {path}")


if __name__ == "__main__":
    main()
