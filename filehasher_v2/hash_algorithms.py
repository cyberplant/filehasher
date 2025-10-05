"""
Hash algorithm implementations and benchmarking
"""

import hashlib
import time
import os
from typing import Callable, Dict, List, Tuple
from pathlib import Path


class HashAlgorithm:
    """Base class for hash algorithms"""
    
    def __init__(self, name: str, hasher_factory: Callable):
        self.name = name
        self.hasher_factory = hasher_factory
    
    def hash_file(self, file_path: Path, chunk_size: int = 65536) -> str:
        """Hash a file in chunks to avoid memory issues"""
        hasher = self.hasher_factory()
        
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except (IOError, OSError) as e:
            raise HashError(f"Failed to hash {file_path}: {e}")
    
    def hash_data(self, data: bytes) -> str:
        """Hash raw data"""
        hasher = self.hasher_factory()
        hasher.update(data)
        return hasher.hexdigest()


class HashError(Exception):
    """Exception raised for hash-related errors"""
    pass


# Available hash algorithms
ALGORITHMS: Dict[str, HashAlgorithm] = {
    'md5': HashAlgorithm('MD5', hashlib.md5),
    'sha1': HashAlgorithm('SHA-1', hashlib.sha1),
    'sha256': HashAlgorithm('SHA-256', hashlib.sha256),
    'sha512': HashAlgorithm('SHA-512', hashlib.sha512),
    'blake2b': HashAlgorithm('BLAKE2b', lambda: hashlib.blake2b(digest_size=32)),
}


def get_algorithm(name: str) -> HashAlgorithm:
    """Get hash algorithm by name"""
    if name.lower() not in ALGORITHMS:
        raise ValueError(f"Unknown hash algorithm: {name}. Available: {', '.join(ALGORITHMS.keys())}")
    return ALGORITHMS[name.lower()]


def benchmark_algorithms(test_file: Path, algorithms: List[str] = None) -> Dict[str, float]:
    """
    Benchmark different hash algorithms on a test file
    
    Returns:
        Dict mapping algorithm names to time taken in seconds
    """
    if algorithms is None:
        algorithms = list(ALGORITHMS.keys())
    
    results = {}
    
    for algo_name in algorithms:
        if algo_name not in ALGORITHMS:
            continue
            
        algorithm = ALGORITHMS[algo_name]
        
        # Warm up
        algorithm.hash_file(test_file)
        
        # Benchmark
        start_time = time.time()
        algorithm.hash_file(test_file)
        end_time = time.time()
        
        results[algo_name] = end_time - start_time
    
    return results


def create_test_file(size_mb: int = 10) -> Path:
    """Create a test file of specified size for benchmarking"""
    test_file = Path("test_benchmark.tmp")
    
    # Create file with random data
    chunk_size = 1024 * 1024  # 1MB chunks
    total_chunks = size_mb
    
    with open(test_file, 'wb') as f:
        for _ in range(total_chunks):
            # Generate random data
            chunk = os.urandom(chunk_size)
            f.write(chunk)
    
    return test_file


def cleanup_test_file(test_file: Path):
    """Clean up test file after benchmarking"""
    try:
        test_file.unlink()
    except OSError:
        pass
