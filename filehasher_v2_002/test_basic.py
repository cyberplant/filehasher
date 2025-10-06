#!/usr/bin/env python3
"""
Basic test script for filehasher v2
"""

import sys
from pathlib import Path
import tempfile
import os

# Add the current directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from hash_algorithms import HashAlgorithm, HashProcessor, HashBenchmark
from file_scanner import FileScanner
from hash_file import HashFile, HashEntry


def test_hash_algorithms():
    """Test hash algorithm functionality"""
    print("Testing hash algorithms...")
    
    # Create a test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write("Hello, World!")
        test_file = Path(f.name)
    
    try:
        # Test different algorithms
        for algorithm in HashAlgorithm:
            processor = HashProcessor(algorithm)
            hash_value = processor.compute_file_hash(test_file)
            print(f"  {algorithm.value}: {hash_value}")
        
        print("✓ Hash algorithms test passed")
        
    finally:
        os.unlink(test_file)


def test_file_scanner():
    """Test file scanner functionality"""
    print("Testing file scanner...")
    
    # Create a temporary directory with test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test files
        (temp_path / "file1.txt").write_text("Content 1")
        (temp_path / "file2.txt").write_text("Content 2")
        (temp_path / "subdir").mkdir()
        (temp_path / "subdir" / "file3.txt").write_text("Content 3")
        
        # Scan directory
        scanner = FileScanner()
        files = scanner.scan_directory(temp_path)
        
        print(f"  Found {len(files)} files")
        for file_info in files:
            print(f"    {file_info.relative_path} ({file_info.size} bytes)")
        
        # Test distribution
        file_lists = scanner.distribute_files_for_processing(2)
        print(f"  Distributed across {len(file_lists)} workers")
        
        print("✓ File scanner test passed")


def test_hash_file():
    """Test hash file functionality"""
    print("Testing hash file...")
    
    # Create test entries
    entries = [
        HashEntry(
            primary_hash="abc123",
            secondary_hash="def456",
            directory=".",
            filename="file1.txt",
            size=100,
            inode=12345,
            mtime=1234567890
        ),
        HashEntry(
            primary_hash="SHA256:xyz789",
            secondary_hash="ghi012",
            directory="subdir",
            filename="file2.txt",
            size=200,
            inode=67890,
            mtime=1234567891
        )
    ]
    
    # Write hash file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.hashes', delete=False) as f:
        temp_file = Path(f.name)
    
    try:
        hash_file = HashFile(temp_file)
        hash_file.write(entries, Path("/tmp"), HashAlgorithm.MD5)
        
        # Read back
        read_entries = hash_file.read()
        print(f"  Wrote and read {len(read_entries)} entries")
        
        # Test duplicates
        duplicates = hash_file.get_duplicates()
        print(f"  Found {len(duplicates)} duplicate groups")
        
        print("✓ Hash file test passed")
        
    finally:
        if temp_file.exists():
            os.unlink(temp_file)


def test_benchmark():
    """Test benchmark functionality"""
    print("Testing benchmark...")
    
    benchmark = HashBenchmark(1)  # 1MB test file
    results = benchmark.benchmark_all([HashAlgorithm.MD5, HashAlgorithm.SHA256], 1)
    
    print(f"  Benchmarked {len(results)} algorithms")
    for result in results:
        print(f"    {result['algorithm']}: {result['throughput_mb_s']:.2f} MB/s")
    
    print("✓ Benchmark test passed")


def main():
    """Run all tests"""
    print("Running filehasher v2 basic tests...\n")
    
    try:
        test_hash_algorithms()
        print()
        
        test_file_scanner()
        print()
        
        test_hash_file()
        print()
        
        test_benchmark()
        print()
        
        print("🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
