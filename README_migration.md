# Hash File Migration Script

This script helps migrate old format hash files to the new format that includes last modification timestamps (mtime). This is necessary for proper file skipping during updates in the filehasher tool.

## Background

The filehasher tool was updated to include timestamps in the hash file format to enable efficient file skipping during updates. The format changed from:

**Old format (5-6 fields):**
```
hashkey|hexdigest|subdir|filename|file_size|file_inode
```

**New format (7 fields):**
```
hashkey|hexdigest|subdir|filename|file_size|file_inode|file_mtime
```

## What the Script Does

1. **Analyzes** the existing hash file to identify entries that need migration
2. **Finds** the corresponding files on disk to get their current modification times
3. **Updates** entries with invalid or missing timestamps
4. **Creates** a backup of the original file before making changes
5. **Preserves** all existing hash data while adding proper timestamps

## Usage

### Basic Usage

```bash
# Migrate the default hash file (.hashes)
python migrate_hashes.py

# Migrate a specific hash file
python migrate_hashes.py --hash-file /path/to/your/hashes.txt

# Migrate with verbose output
python migrate_hashes.py --verbose

# Dry run to see what would be migrated
python migrate_hashes.py --dry-run --verbose
```

### Command Line Options

- `--hash-file PATH, -f PATH`: Path to the hash file to migrate (default: `.hashes`)
- `--directory DIR, -d DIR`: Root directory to search for files (default: current directory)
- `--verbose, -v`: Enable verbose output showing each file being processed
- `--dry-run, -n`: Show what would be migrated without making changes
- `--help, -h`: Show help message

## Examples

### Example 1: Basic Migration

```bash
cd /path/to/your/project
python migrate_hashes.py --hash-file myhashes.txt --verbose
```

### Example 2: Dry Run First

```bash
# See what would be migrated without making changes
python migrate_hashes.py --dry-run --verbose

# Then perform the actual migration
python migrate_hashes.py --verbose
```

### Example 3: Custom Directory

```bash
# Migrate hashes for files in a specific directory
python migrate_hashes.py --directory /home/user/documents --hash-file docs.hashes
```

## What Gets Migrated

The script identifies entries that need migration based on:

1. **Old format entries**: Lines with fewer than 7 fields (missing mtime)
2. **Invalid timestamps**: Entries with timestamp 0 or timestamps before 1990 (likely from archives)
3. **Missing timestamps**: Entries with missing mtime field

## Safety Features

- **Automatic backup**: Creates `.hashes.backup` before making changes
- **Dry run mode**: Test what would be migrated without making changes
- **File validation**: Only updates entries where the corresponding file can be found
- **Preservation**: All existing hash data is preserved, only timestamps are updated

## Output

The script provides detailed output including:

- Number of entries loaded
- Number of entries requiring migration
- Progress updates during migration (with `--verbose`)
- Final summary with backup location

## Troubleshooting

### Files Not Found

If files referenced in the hash file cannot be found, the script will:
- Show a warning message
- Keep the existing timestamp value
- Continue processing other files

### Permission Errors

If you encounter permission errors:
- Ensure you have write access to the hash file location
- Ensure you have read access to the files being processed

### Large Files

For very large hash files, the migration may take some time. The `--verbose` option shows progress for each file being processed.

## Integration with Filehasher

After migration, your hash files will work seamlessly with the updated filehasher tool, enabling:

- Faster updates by skipping unchanged files
- Better reliability for files with proper timestamps
- Backward compatibility with the existing codebase

## Technical Details

- The script uses the same hash key format as the filehasher tool
- Timestamps are stored as Unix timestamps (seconds since epoch)
- The script searches multiple directories to find files referenced in the hash file
- All file operations include proper error handling

## Requirements

- Python 3.6+
- Read/write access to the hash file
- Read access to the files being processed

## Related Files

- `migrate_hashes.py`: The migration script
- `filehasher/__init__.py`: The main filehasher module (for reference)
- `.hashes`: Default hash file location
- `.hashes.backup`: Automatic backup created during migration
