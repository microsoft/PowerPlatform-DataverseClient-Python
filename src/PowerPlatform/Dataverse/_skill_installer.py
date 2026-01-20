# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Claude Code skill installer for the PowerPlatform Dataverse Client SDK.

This module provides a CLI command to install the Dataverse SDK Claude Skill
for Claude Code
"""

import shutil
import sys
from pathlib import Path

# Ensure UTF-8 output for emoji support on Windows
if sys.platform == "win32":
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


def get_skill_source_path() -> Path:
    """Get the path to the skill source directory in the package."""
    # Two locations for the skill:
    # 1. .claude/skills/dataverse-sdk/ (repo root, for development)
    # 2. claude_skill/ (in package, for PyPI distribution)

    package_dir = Path(__file__).parent  # PowerPlatform/Dataverse/

    # Try development/repo location first (for pip install -e .)
    # Go up to repo root: Dataverse -> PowerPlatform -> src -> repo root
    repo_root = package_dir.parent.parent.parent
    repo_claude_path = repo_root / ".claude" / "skills" / "dataverse-sdk"
    if repo_claude_path.exists():
        return repo_claude_path

    # Try packaged location (for pip install from wheel/sdist)
    # The skill is packaged in PowerPlatform/Dataverse/claude_skill/
    packaged_skill_path = package_dir / "claude_skill"
    if packaged_skill_path.exists():
        return packaged_skill_path

    # Fallback: return repo path even if it doesn't exist
    # (will be caught by validation in install_skill)
    return repo_claude_path


def get_skill_destination_path() -> Path:
    """Get the destination path for installing the skill globally."""
    return Path.home() / ".claude" / "skills" / "dataverse-sdk"


def install_skill(force: bool = False) -> bool:
    """
    Install the Dataverse SDK skill for Claude Code.

    Args:
        force: If True, overwrite existing skill without prompting.

    Returns:
        True if installation succeeded, False otherwise.
    """
    skill_source = get_skill_source_path()
    skill_dest = get_skill_destination_path()

    # Validate source exists
    if not skill_source.exists():
        print(f"❌ Error: Skill source not found at {skill_source}")
        print("   This may indicate a packaging issue.")
        return False

    skill_md = skill_source / "SKILL.md"
    if not skill_md.exists():
        print(f"❌ Error: SKILL.md not found at {skill_md}")
        return False

    # Check if skill already exists
    if skill_dest.exists():
        if not force:
            print(f"⚠️  Skill already exists at {skill_dest}")
            response = input("   Overwrite existing skill? (y/n): ").strip().lower()
            if response not in ["y", "yes"]:
                print("   Installation cancelled.")
                return False
        print(f"   Updating existing skill...")

    # Create destination directory
    skill_dest.parent.mkdir(parents=True, exist_ok=True)

    # Copy skill files
    try:
        if skill_dest.exists():
            shutil.rmtree(skill_dest)
        shutil.copytree(skill_source, skill_dest)
        print(f"✅ Dataverse SDK skill installed successfully!")
        print(f"   Location: {skill_dest}")
        print()
        print("   Claude Code will now automatically use this skill when working")
        print("   with the PowerPlatform Dataverse Client SDK.")
        print()
        print("💡 Next steps:")
        print("   • Start Claude Code in your project directory")
        print("   • Ask Claude for help with Dataverse operations")
        print("   • Claude will automatically apply SDK best practices")
        return True
    except Exception as e:
        print(f"❌ Error installing skill: {e}")
        return False


def uninstall_skill() -> bool:
    """
    Uninstall the Dataverse SDK skill from Claude Code.

    Returns:
        True if uninstallation succeeded, False otherwise.
    """
    skill_dest = get_skill_destination_path()

    if not skill_dest.exists():
        print(f"ℹ️  Skill not found at {skill_dest}")
        print("   Nothing to uninstall.")
        return True

    try:
        shutil.rmtree(skill_dest)
        print(f"✅ Dataverse SDK skill uninstalled successfully!")
        print(f"   Removed from: {skill_dest}")
        return True
    except Exception as e:
        print(f"❌ Error uninstalling skill: {e}")
        return False


def check_skill_status() -> None:
    """Check and display the current skill installation status."""
    skill_dest = get_skill_destination_path()

    print("🔍 Dataverse SDK Skill Status")
    print("=" * 60)

    if skill_dest.exists():
        skill_md = skill_dest / "SKILL.md"
        if skill_md.exists():
            print(f"✅ Status: Installed")
            print(f"   Location: {skill_dest}")
            print(f"   Skill file: {skill_md}")
            print()
            print("   The skill is ready to use with Claude Code.")
        else:
            print(f"⚠️  Status: Partially installed (SKILL.md missing)")
            print(f"   Location: {skill_dest}")
            print()
            print("   Consider reinstalling: dataverse-install-claude-skill --force")
    else:
        print(f"❌ Status: Not installed")
        print(f"   Expected location: {skill_dest}")
        print()
        print("   To install: dataverse-install-claude-skill")


def main() -> None:
    """Main entry point for the skill installer CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Install the Dataverse SDK skill for Claude Code",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Install the skill (will prompt if exists)
  dataverse-install-claude-skill

  # Force install (overwrite without prompting)
  dataverse-install-claude-skill --force

  # Check installation status
  dataverse-install-claude-skill --status

  # Uninstall the skill
  dataverse-install-claude-skill --uninstall

About:
  This command installs a Claude Code skill that provides expert guidance
  for using the PowerPlatform Dataverse Client Python SDK. Once installed,
  Claude Code will automatically apply SDK best practices and provide
  intelligent assistance when working with Dataverse.
        """,
    )

    parser.add_argument(
        "--force", "-f", action="store_true", help="Force install without prompting (overwrite existing)"
    )

    parser.add_argument("--status", "-s", action="store_true", help="Check skill installation status")

    parser.add_argument("--uninstall", "-u", action="store_true", help="Uninstall the skill")

    args = parser.parse_args()

    print()
    print("🚀 PowerPlatform Dataverse SDK - Claude Code Skill Installer")
    print("=" * 60)
    print()

    # Handle different modes
    if args.status:
        check_skill_status()
        sys.exit(0)

    if args.uninstall:
        success = uninstall_skill()
        sys.exit(0 if success else 1)

    # Default: Install
    success = install_skill(force=args.force)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
