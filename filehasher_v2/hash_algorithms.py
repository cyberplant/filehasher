"""
Hash algorithm implementations and benchmarking
"""

import hashlib
import time
import os
import tempfile
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Supported hash algorithms"""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"


class HashProcessor:
    """Handles hash computation for different algorithms"""
    
    # Default chunk size for reading files (1MB)
    CHUNK_SIZE = 1024 * 1024
    
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.MD5):
        self.algorithm = algorithm
        self._hash_obj = None
        self._reset_hash()
    
    def _reset_hash(self):
        """Reset the hash object for a new computation"""
        if self.algorithm == HashAlgorithm.MD5:
            self._hash_obj = hashlib.md5()
        elif self.algorithm == HashAlgorithm.SHA1:
            self._hash_obj = hashlib.sha1()
        elif self.algorithm == HashAlgorithm.SHA256:
            self._hash_obj = hashlib.sha256()
        elif self.algorithm == HashAlgorithm.SHA512:
            self._hash_obj = hashlib.sha512()
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            self._hash_obj = hashlib.blake2b()
        elif self.algorithm == HashAlgorithm.BLAKE2S:
            self._hash_obj = hashlib.blake2s()
        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")
    
    def compute_file_hash(self, file_path: Path, progress_callback=None) -> str:
        """
        Compute hash of a file
        
        Args:
            file_path: Path to the file
            progress_callback: Optional callback for progress updates (bytes_processed, total_bytes)
        
        Returns:
            Hash string in format "algorithm:hash_value" or just "hash_value" for MD5
        """
        try:
            file_size = file_path.stat().st_size
            bytes_processed = 0
            
            with open(file_path, 'rb') as f:
                while chunk := f.read(self.CHUNK_SIZE):
                    self._hash_obj.update(chunk)
                    bytes_processed += len(chunk)
                    
                    if progress_callback:
                        progress_callback(bytes_processed, file_size)
            
            hash_value = self._hash_obj.hexdigest()
            
            # Format hash with algorithm prefix (except MD5 for backwards compatibility)
            if self.algorithm == HashAlgorithm.MD5:
                return hash_value
            else:
                return f"{self.algorithm.value.upper()}:{hash_value}"
                
        except Exception as e:
            logger.error(f"Error computing hash for {file_path}: {e}")
            raise
        finally:
            self._reset_hash()
    
    def compute_data_hash(self, data: bytes) -> str:
        """Compute hash of raw data"""
        hash_value = self._hash_obj.update(data) or self._hash_obj.hexdigest()
        
        if self.algorithm == HashAlgorithm.MD5:
            return hash_value
        else:
            return f"{self.algorithm.value.upper()}:{hash_value}"
    
    @staticmethod
    def parse_hash_string(hash_string: str) -> Tuple[HashAlgorithm, str]:
        """
        Parse a hash string to extract algorithm and hash value
        
        Args:
            hash_string: Hash string in format "algorithm:hash_value" or just "hash_value" for MD5
        
        Returns:
            Tuple of (algorithm, hash_value)
        """
        if ':' in hash_string:
            algorithm_str, hash_value = hash_string.split(':', 1)
            try:
                algorithm = HashAlgorithm(algorithm_str.lower())
            except ValueError:
                # Fallback to MD5 if algorithm not recognized
                algorithm = HashAlgorithm.MD5
                hash_value = hash_string
        else:
            # No prefix, assume MD5 for backwards compatibility
            algorithm = HashAlgorithm.MD5
            hash_value = hash_string
        
        return algorithm, hash_value


class HashBenchmark:
    """Benchmark different hash algorithms"""
    
    def __init__(self, test_size_mb: int = 10):
        self.test_size_mb = test_size_mb
        self.test_size_bytes = test_size_mb * 1024 * 1024
    
    def _create_test_data(self) -> bytes:
        """Create test data of specified size"""
        # Generate pseudo-random data
        data = bytearray()
        pattern = b"abcdefghijklmnopqrstuvwxyz0123456789" * 1000
        
        while len(data) < self.test_size_bytes:
            remaining = self.test_size_bytes - len(data)
            data.extend(pattern[:remaining])
        
        return bytes(data)
    
    def _create_test_file(self) -> Path:
        """Create a temporary test file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_data = self._create_test_data()
            f.write(test_data)
            return Path(f.name)
    
    def benchmark_algorithm(self, algorithm: HashAlgorithm, iterations: int = 3) -> Dict:
        """
        Benchmark a single algorithm
        
        Args:
            algorithm: Hash algorithm to benchmark
            iterations: Number of iterations to run
        
        Returns:
            Dictionary with benchmark results
        """
        times = []
        test_file = None
        
        try:
            test_file = self._create_test_file()
            processor = HashProcessor(algorithm)
            
            for _ in range(iterations):
                start_time = time.time()
                processor.compute_file_hash(test_file)
                end_time = time.time()
                times.append(end_time - start_time)
            
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            throughput = self.test_size_bytes / avg_time / (1024 * 1024)  # MB/s
            
            return {
                'algorithm': algorithm.value,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'throughput_mb_s': throughput,
                'iterations': iterations
            }
            
        finally:
            if test_file and test_file.exists():
                try:
                    os.unlink(test_file)
                except OSError:
                    pass
    
    def benchmark_all(self, algorithms: Optional[List[HashAlgorithm]] = None, 
                     iterations: int = 3) -> List[Dict]:
        """
        Benchmark all supported algorithms
        
        Args:
            algorithms: List of algorithms to benchmark (None for all)
            iterations: Number of iterations per algorithm
        
        Returns:
            List of benchmark results
        """
        if algorithms is None:
            algorithms = list(HashAlgorithm)
        
        results = []
        for algorithm in algorithms:
            logger.info(f"Benchmarking {algorithm.value}...")
            result = self.benchmark_algorithm(algorithm, iterations)
            results.append(result)
        
        # Sort by throughput (descending)
        results.sort(key=lambda x: x['throughput_mb_s'], reverse=True)
        
        return results
    
    def get_recommendation(self, results: List[Dict]) -> str:
        """Get algorithm recommendation based on benchmark results"""
        if not results:
            return "No benchmark results available"
        
        fastest = results[0]
        recommended = "SHA256"  # Default recommendation
        
        # If MD5 is significantly faster, recommend it for speed
        md5_result = next((r for r in results if r['algorithm'] == 'md5'), None)
        if md5_result and md5_result['throughput_mb_s'] > fastest['throughput_mb_s'] * 1.5:
            recommended = "MD5 (fastest, but less secure)"
        elif fastest['algorithm'] == 'sha256':
            recommended = "SHA256 (fastest secure algorithm)"
        elif fastest['algorithm'] == 'blake2b':
            recommended = "Blake2B (modern, fast, and secure)"
        elif fastest['algorithm'] == 'sha1':
            recommended = "SHA1 (fast but deprecated)"
        
        return recommended


def get_algorithm_from_string(algorithm_str: str) -> HashAlgorithm:
    """Convert string to HashAlgorithm enum"""
    try:
        return HashAlgorithm(algorithm_str.lower())
    except ValueError:
        raise ValueError(f"Unsupported algorithm: {algorithm_str}. "
                        f"Supported algorithms: {[a.value for a in HashAlgorithm]}")


def format_hash_for_display(hash_string: str, max_length: int = 16) -> str:
    """Format hash string for display, truncating if too long"""
    if len(hash_string) <= max_length:
        return hash_string
    
    if ':' in hash_string:
        algorithm, hash_value = hash_string.split(':', 1)
        truncated_hash = hash_value[:max_length - len(algorithm) - 2] + "..."
        return f"{algorithm}:{truncated_hash}"
    else:
        return hash_string[:max_length - 3] + "..."
