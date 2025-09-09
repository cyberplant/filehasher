#!/usr/bin/env python3
"""
Hash File Migration Script

This script migrates old format hash files to the new format that includes
last modification timestamps (mtime). The migration is necessary for proper
file skipping during updates.

Old format (5-6 fields): hashkey|hexdigest|subdir|filename|file_size|file_inode
New format (7 fields): hashkey|hexdigest|subdir|filename|file_size|file_inode|file_mtime

Usage:
    python migrate_hashes.py [--hash-file PATH] [--directory DIR] [--verbose]

Arguments:
    --hash-file PATH    Path to the hash file to migrate (default: .hashes)
    --directory DIR     Root directory to search for files (default: current directory)
    --verbose           Enable verbose output
    --dry-run          Show what would be migrated without making changes

The script will:
1. Load the existing hash file
2. Identify entries in old format (missing mtime)
3. Find the corresponding files and get their current mtime
4. Update the hash file with the new format
5. Preserve all existing data
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def load_hash_file(hash_file: str) -> Tuple[Dict[str, Tuple], Optional[str]]:
    """Load hash file and return dictionary and algorithm."""
    hashes = {}
    algorithm = None

    if not os.path.exists(hash_file):
        print(f"Hash file {hash_file} does not exist.")
        return hashes, algorithm

    print(f"Loading hash file: {hash_file}")

    with open(hash_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip()
            if not line:
                continue

            # Check for algorithm header
            if line.startswith("# Algorithm: "):
                algorithm = line[13:].strip()
                continue

            # Parse hash entry
            parts = line.split("|")
            if len(parts) >= 5:
                hashkey = parts[0]
                hexdigest = parts[1]
                subdir = parts[2]
                filename = parts[3]
                file_size = int(parts[4]) if len(parts) > 4 else 0
                file_inode = int(parts[5]) if len(parts) > 5 else 0
                file_mtime = float(parts[6]) if len(parts) > 6 else 0

                hashes[hashkey] = (hexdigest, subdir, filename, file_size, file_inode, file_mtime)

    print(f"Loaded {len(hashes)} hash entries")
    if algorithm:
        print(f"Algorithm: {algorithm}")

    return hashes, algorithm


def find_file_for_hashkey(hashkey: str, search_dirs: List[str], verbose: bool = False) -> Optional[str]:
    """Find the actual file path for a given hashkey by searching in directories."""
    # Convert hashkey back to relative path
    rel_path = hashkey.replace("/", os.sep)

    for search_dir in search_dirs:
        candidate_path = os.path.join(search_dir, rel_path)
        if os.path.exists(candidate_path) and os.path.isfile(candidate_path):
            if verbose:
                print(f"  Found file: {candidate_path}")
            return candidate_path

    if verbose:
        print(f"  File not found for hashkey: {hashkey}")
    return None


def get_file_mtime(filepath: str) -> Optional[float]:
    """Get the modification time of a file."""
    try:
        stat = os.stat(filepath)
        return stat.st_mtime
    except OSError as e:
        print(f"Error getting mtime for {filepath}: {e}")
        return None


def identify_entries_needing_migration(hashes: Dict[str, Tuple]) -> List[str]:
    """Identify hash entries that need migration (missing or invalid mtime)."""
    needs_migration = []

    for hashkey, data in hashes.items():
        if len(data) < 6:
            # Old format with fewer than 6 fields
            needs_migration.append(hashkey)
        elif data[5] == 0 or (data[5] > 0 and data[5] < 631152000):  # Before 1990 or invalid
            # Invalid timestamp that should be updated
            needs_migration.append(hashkey)

    return needs_migration


def migrate_hash_file(hash_file: str, search_dirs: List[str], verbose: bool = False, dry_run: bool = False) -> int:
    """Migrate hash file to new format with timestamps."""
    # Load existing hashes
    hashes, algorithm = load_hash_file(hash_file)

    if not hashes:
        print("No hashes to migrate.")
        return 0

    # Identify entries needing migration
    needs_migration = identify_entries_needing_migration(hashes)

    if not needs_migration:
        print("✅ All entries are already in the new format with valid timestamps.")
        return 0

    print(f"Found {len(needs_migration)} entries that need migration.")

    if dry_run:
        print("\n📋 DRY RUN - The following entries would be migrated:")
        for hashkey in needs_migration[:10]:  # Show first 10
            data = hashes[hashkey]
            print(f"  {hashkey} (fields: {len(data)}, mtime: {data[5] if len(data) > 5 else 'N/A'})")
        if len(needs_migration) > 10:
            print(f"  ... and {len(needs_migration) - 10} more")
        print(f"\nTotal entries that would be migrated: {len(needs_migration)}")
        return len(needs_migration)

    # Perform migration
    migrated_count = 0
    updated_hashes = {}

    for hashkey in hashes:
        data = hashes[hashkey]

        if hashkey in needs_migration:
            # This entry needs migration
            if verbose:
                print(f"Migrating: {hashkey}")

            # Try to find the file and get its current mtime
            file_path = find_file_for_hashkey(hashkey, search_dirs, verbose)
            new_mtime = None

            if file_path:
                new_mtime = get_file_mtime(file_path)
                if new_mtime is not None:
                    if verbose:
                        print(f"  Updated mtime: {data[5] if len(data) > 5 else 'N/A'} -> {new_mtime}")
                else:
                    print(f"  ⚠️  Could not get mtime for {file_path}, keeping old value")
                    new_mtime = data[5] if len(data) > 5 else 0
            else:
                print(f"  ⚠️  File not found for {hashkey}, keeping old value")
                new_mtime = data[5] if len(data) > 5 else 0

            # Create new data tuple with proper mtime
            if len(data) >= 6:
                # Has inode, update mtime
                updated_hashes[hashkey] = (data[0], data[1], data[2], data[3], data[4], data[5], new_mtime)
            else:
                # Missing fields, add them
                file_inode = 0  # Default value
                updated_hashes[hashkey] = (data[0], data[1], data[2], data[3], data[4] if len(data) > 4 else 0, file_inode, new_mtime)

            migrated_count += 1
        else:
            # This entry is already good
            updated_hashes[hashkey] = data

    # Write the migrated hash file
    if migrated_count > 0:
        backup_file = hash_file + ".backup"
        if verbose:
            print(f"Creating backup: {backup_file}")

        # Create backup
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as src, open(backup_file, 'w') as dst:
                dst.write(src.read())

        # Write new hash file
        with open(hash_file, 'w') as f:
            if algorithm:
                f.write(f"# Algorithm: {algorithm}\n")

            for hashkey, data in sorted(updated_hashes.items()):
                if len(data) >= 7:
                    line = f"{hashkey}|{data[0]}|{data[1]}|{data[2]}|{data[3]}|{data[4]}|{data[5]}"
                else:
                    # Fallback for any remaining old format entries
                    line = f"{hashkey}|{data[0]}|{data[1]}|{data[2]}|{data[3]}"
                    if len(data) > 4:
                        line += f"|{data[4]}"
                    if len(data) > 5:
                        line += f"|{data[5]}"
                f.write(line + "\n")

        print(f"✅ Migration completed! Updated {migrated_count} entries.")
        print(f"📁 Backup created: {backup_file}")

    return migrated_count


def main():
    parser = argparse.ArgumentParser(
        description="Migrate hash files from old format to new format with timestamps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--hash-file', '-f',
        default='.hashes',
        help='Path to the hash file to migrate (default: .hashes)'
    )

    parser.add_argument(
        '--directory', '-d',
        default='.',
        help='Root directory to search for files (default: current directory)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be migrated without making changes'
    )

    args = parser.parse_args()

    # Validate inputs
    if not os.path.exists(args.directory):
        print(f"Error: Directory {args.directory} does not exist.")
        sys.exit(1)

    # Prepare search directories
    search_dirs = [os.path.abspath(args.directory)]

    # Also search in common locations
    home_dir = os.path.expanduser("~")
    if home_dir not in search_dirs:
        search_dirs.append(home_dir)

    if args.verbose:
        print(f"Search directories: {search_dirs}")

    # Perform migration
    try:
        migrated = migrate_hash_file(
            hash_file=args.hash_file,
            search_dirs=search_dirs,
            verbose=args.verbose,
            dry_run=args.dry_run
        )

        if args.dry_run:
            print(f"\n📊 Summary: {migrated} entries would be migrated.")
        else:
            print(f"\n📊 Summary: {migrated} entries were migrated.")

    except KeyboardInterrupt:
        print("\n⚠️  Migration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
