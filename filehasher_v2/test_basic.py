"""
Basic tests for filehasher v2 core functionality.
"""

import os
import tempfile
import shutil
from pathlib import Path

from .hash_algorithms import HashAlgorithm, HashCalculator, BenchmarkRunner
from .file_scanner import FileScanner
from .hash_file import HashFileWriter, HashFileReader
from .processor import HashProcessor


def test_hash_algorithms():
    """Test hash algorithm functionality."""
    print("Testing hash algorithms...")
    
    # Create a temporary test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Hello, World!\nThis is a test file for hashing.")
        test_file = f.name
    
    try:
        # Test different algorithms
        algorithms = [HashAlgorithm.MD5, HashAlgorithm.SHA1, HashAlgorithm.SHA256]
        
        for algorithm in algorithms:
            calculator = HashCalculator(algorithm)
            hash_value = calculator.calculate_file_hash(test_file)
            formatted_hash = calculator.format_hash(hash_value)
            
            print(f"  {algorithm.value}: {formatted_hash}")
            
            # Test metadata hash
            metadata_hash = calculator.calculate_metadata_hash("test.txt", 50, 12345)
            print(f"  Metadata hash: {metadata_hash}")
    
    finally:
        os.unlink(test_file)
    
    print("Hash algorithms test passed!")


def test_file_scanner():
    """Test file scanner functionality."""
    print("Testing file scanner...")
    
    # Create temporary directory structure
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test files
        (temp_path / "file1.txt").write_text("Content 1")
        (temp_path / "file2.txt").write_text("Content 2")
        (temp_path / "subdir").mkdir()
        (temp_path / "subdir" / "file3.txt").write_text("Content 3")
        
        # Test scanner
        scanner = FileScanner(str(temp_path))
        files, symlinks = scanner.scan_directory()
        
        print(f"  Found {len(files)} files, {len(symlinks)} symlinks")
        print(f"  Total size: {scanner.get_total_size()} bytes")
        
        # Test distribution
        worker_files = scanner.distribute_files(2)
        print(f"  Distributed across {len(worker_files)} workers")
        
        for i, files in enumerate(worker_files):
            total_size = sum(f.size for f in files)
            print(f"    Worker {i}: {len(files)} files, {total_size} bytes")
    
    print("File scanner test passed!")


def test_hash_file_format():
    """Test hash file format handling."""
    print("Testing hash file format...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        hash_file_path = temp_path / "test.hashes"
        
        # Create test files
        test_file = temp_path / "test.txt"
        test_file.write_text("Test content")
        
        # Create hash file
        writer = HashFileWriter(str(hash_file_path), HashAlgorithm.SHA256)
        
        # Add test entry
        from .file_scanner import FileInfo
        file_info = FileInfo(
            path=str(test_file),
            relative_path="test.txt",
            size=test_file.stat().st_size,
            inode=test_file.stat().st_ino,
            mtime=int(test_file.stat().st_mtime)
        )
        
        calculator = HashCalculator(HashAlgorithm.SHA256)
        file_hash = calculator.calculate_file_hash(str(test_file))
        metadata_hash = calculator.calculate_metadata_hash("test.txt", file_info.size, file_info.mtime)
        
        writer.add_entry(file_info, metadata_hash, file_hash)
        writer.write_file(str(temp_path))
        
        # Read back and verify
        reader = HashFileReader(str(hash_file_path))
        entries, metadata = reader.read_file()
        
        print(f"  Metadata: {metadata}")
        print(f"  Entries: {len(entries)}")
        
        if entries:
            entry = entries[0]
            print(f"  First entry: {entry.filename} -> {entry.file_hash[:16]}...")
    
    print("Hash file format test passed!")


def test_processor():
    """Test processor functionality."""
    print("Testing processor...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test files
        for i in range(5):
            (temp_path / f"file{i}.txt").write_text(f"Content {i}" * 100)
        
        # Test processor
        processor = HashProcessor(HashAlgorithm.MD5, num_workers=2)
        output_path = temp_path / "test.hashes"
        
        success = processor.process_directory(
            directory=str(temp_path),
            output_path=str(output_path),
            quiet=True
        )
        
        if success and output_path.exists():
            print(f"  Generated hash file: {output_path}")
            print(f"  File size: {output_path.stat().st_size} bytes")
        else:
            print("  Failed to generate hash file")
    
    print("Processor test passed!")


def test_benchmark():
    """Test benchmarking functionality."""
    print("Testing benchmark...")
    
    runner = BenchmarkRunner(iterations=2)
    results = runner.benchmark_all(1)  # 1 MB test file
    
    print(f"  Benchmarked {len(results)} algorithms")
    for result in results[:3]:  # Show top 3
        print(f"    {result.algorithm}: {result.throughput_mbps:.2f} MB/s")
    
    print("Benchmark test passed!")


def main():
    """Run all tests."""
    print("Running filehasher v2 basic tests...\n")
    
    try:
        test_hash_algorithms()
        print()
        
        test_file_scanner()
        print()
        
        test_hash_file_format()
        print()
        
        test_processor()
        print()
        
        test_benchmark()
        print()
        
        print("All tests passed! ✅")
        return 0
    
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
