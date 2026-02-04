# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Claude Code skill installer for the PowerPlatform Dataverse Client SDK.

This module provides a CLI command to install the Dataverse SDK Claude Skills
for Claude Code. Two skills are installed:
- dataverse-sdk-use: For using the SDK in your applications
- dataverse-sdk-dev: For developing/contributing to the SDK itself
"""

import shutil
import sys
from pathlib import Path


def get_skill_source_paths() -> dict[str, Path]:
    """Get the paths to the skill source directories in the package."""
    # Two locations for skills:
    # 1. .claude/skills/ (repo root, for development)
    # 2. claude_skill/ (in package, for PyPI distribution)

    package_dir = Path(__file__).parent  # PowerPlatform/Dataverse/

    # Try development/repo location first (for pip install -e .)
    # Go up to repo root: Dataverse -> PowerPlatform -> src -> repo root
    repo_root = package_dir.parent.parent.parent
    repo_claude_base = repo_root / ".claude" / "skills"

    skills = {}

    # Check if we're in development mode (repo has .claude/skills/)
    if repo_claude_base.exists():
        skills["dataverse-sdk-use"] = repo_claude_base / "dataverse-sdk-use"
        skills["dataverse-sdk-dev"] = repo_claude_base / "dataverse-sdk-dev"
    else:
        # Try packaged location (for pip install from wheel/sdist)
        # Skills are packaged in PowerPlatform/Dataverse/claude_skill/
        packaged_skill_base = package_dir / "claude_skill"
        skills["dataverse-sdk-use"] = packaged_skill_base / "dataverse-sdk-use"
        skills["dataverse-sdk-dev"] = packaged_skill_base / "dataverse-sdk-dev"

    return skills


def get_skill_destination_paths() -> dict[str, Path]:
    """Get the destination paths for installing the skills globally."""
    claude_skills_dir = Path.home() / ".claude" / "skills"
    return {
        "dataverse-sdk-use": claude_skills_dir / "dataverse-sdk-use",
        "dataverse-sdk-dev": claude_skills_dir / "dataverse-sdk-dev",
    }


def install_skill(force: bool = False) -> bool:
    """
    Install the Dataverse SDK skills for Claude Code.

    Args:
        force: If True, overwrite existing skills without prompting.

    Returns:
        True if installation succeeded, False otherwise.
    """
    skill_sources = get_skill_source_paths()
    skill_dests = get_skill_destination_paths()

    # Track installation results
    all_success = True
    installed_count = 0

    for skill_name, skill_source in skill_sources.items():
        skill_dest = skill_dests[skill_name]

        # Validate source exists
        if not skill_source.exists():
            print(f"[ERR] Skill source not found: {skill_name}")
            print(f"        Path: {skill_source}")
            all_success = False
            continue

        skill_md = skill_source / "SKILL.md"
        if not skill_md.exists():
            print(f"[ERR] SKILL.md not found for {skill_name}")
            print(f"        Path: {skill_md}")
            all_success = False
            continue

        # Check if skill already exists
        if skill_dest.exists():
            if not force:
                print(f"[WARN] Skill '{skill_name}' already exists at {skill_dest}")
                response = input("       Overwrite? (y/n): ").strip().lower()
                if response not in ["y", "yes"]:
                    print("       Skipping this skill...")
                    continue
            print(f"       Updating '{skill_name}'...")

        # Create destination directory
        skill_dest.parent.mkdir(parents=True, exist_ok=True)

        # Copy skill files
        try:
            if skill_dest.exists():
                shutil.rmtree(skill_dest)
            shutil.copytree(skill_source, skill_dest)
            print(f"[OK] Installed '{skill_name}'")
            print(f"     Location: {skill_dest}")
            installed_count += 1
        except Exception as e:
            print(f"[ERR] Failed to install '{skill_name}': {e}")
            all_success = False

    # Print summary
    if installed_count > 0:
        print()
        print(f"[OK] Successfully installed {installed_count} skill(s)!")
        print()
        print("     Claude Code will now automatically use these skills:")
        print("     * dataverse-sdk-use - For using the SDK in your applications")
        print("     * dataverse-sdk-dev - For developing/contributing to the SDK")
        print()
        print("[INFO] Next steps:")
        print("       * Start Claude Code in your project directory")
        print("       * Ask Claude for help with Dataverse operations")
        print("       * Claude will automatically apply the appropriate skill")

    return all_success and installed_count > 0


def uninstall_skill() -> bool:
    """
    Uninstall the Dataverse SDK skills from Claude Code.

    Returns:
        True if uninstallation succeeded, False otherwise.
    """
    skill_dests = get_skill_destination_paths()

    uninstalled_count = 0
    all_success = True

    for skill_name, skill_dest in skill_dests.items():
        if not skill_dest.exists():
            print(f"[INFO] Skill '{skill_name}' not found - nothing to uninstall")
            continue

        try:
            shutil.rmtree(skill_dest)
            print(f"[OK] Uninstalled '{skill_name}'")
            print(f"     Removed from: {skill_dest}")
            uninstalled_count += 1
        except Exception as e:
            print(f"[ERR] Failed to uninstall '{skill_name}': {e}")
            all_success = False

    if uninstalled_count == 0:
        print("[INFO] No skills were installed")
        return True

    print()
    print(f"[OK] Successfully uninstalled {uninstalled_count} skill(s)!")
    return all_success


def check_skill_status() -> None:
    """Check and display the current skill installation status."""
    skill_dests = get_skill_destination_paths()

    print("[INFO] Dataverse SDK Skills Status")
    print("=" * 60)

    installed_count = 0
    for skill_name, skill_dest in skill_dests.items():
        print(f"\nSkill: {skill_name}")
        print("-" * 60)

        if skill_dest.exists():
            skill_md = skill_dest / "SKILL.md"
            if skill_md.exists():
                print(f"[OK] Status: Installed")
                print(f"     Location: {skill_dest}")
                print(f"     Skill file: {skill_md}")
                installed_count += 1
            else:
                print(f"[WARN] Status: Partially installed (SKILL.md missing)")
                print(f"       Location: {skill_dest}")
        else:
            print(f"[INFO] Status: Not installed")
            print(f"       Expected location: {skill_dest}")

    print()
    print("=" * 60)
    if installed_count == len(skill_dests):
        print("[OK] All skills are installed and ready to use!")
    elif installed_count > 0:
        print(f"[WARN] {installed_count}/{len(skill_dests)} skills installed")
        print("       Run: dataverse-install-claude-skill --force")
    else:
        print("[INFO] No skills installed")
        print("       Run: dataverse-install-claude-skill")


def main() -> None:
    """Main entry point for the skill installer CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Install the Dataverse SDK skills for Claude Code",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Install the skills (will prompt if exists)
  dataverse-install-claude-skill

  # Force install (overwrite without prompting)
  dataverse-install-claude-skill --force

  # Check installation status
  dataverse-install-claude-skill --status

  # Uninstall the skills
  dataverse-install-claude-skill --uninstall

About:
  This command installs two Claude Code skills for the PowerPlatform Dataverse
  Client Python SDK:

  * dataverse-sdk-use - Guidance for using the SDK in your applications
  * dataverse-sdk-dev - Guidance for developing/contributing to the SDK itself

  Once installed, Claude Code will automatically apply the appropriate skill
  and provide intelligent assistance when working with Dataverse.
        """,
    )

    parser.add_argument(
        "--force", "-f", action="store_true", help="Force install without prompting (overwrite existing)"
    )

    parser.add_argument("--status", "-s", action="store_true", help="Check skill installation status")

    parser.add_argument("--uninstall", "-u", action="store_true", help="Uninstall the skill")

    args = parser.parse_args()

    print()
    print("PowerPlatform Dataverse SDK - Claude Code Skill Installer")
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
