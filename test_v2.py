#!/usr/bin/env python3
"""
Simple test script for filehasher v2
"""

import tempfile
import os
from pathlib import Path

# Create a test directory with some files
test_dir = Path("test_files")
test_dir.mkdir(exist_ok=True)

# Create some test files
(test_dir / "file1.txt").write_text("This is file 1")
(test_dir / "file2.txt").write_text("This is file 2")
(test_dir / "file3.txt").write_text("This is file 1")  # Duplicate content
(test_dir / "subdir").mkdir(exist_ok=True)
(test_dir / "subdir" / "file4.txt").write_text("This is file 4")

print("Created test files:")
for file in test_dir.rglob("*"):
    if file.is_file():
        print(f"  {file}")

print("\nTest directory structure ready!")
print("You can now test the filehasher v2 with:")
print(f"  python main.py generate {test_dir}")
print(f"  python main.py duplicates {test_dir}.hashes")
print(f"  python main.py benchmark")
