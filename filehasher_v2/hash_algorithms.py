"""
Hash algorithm implementations and benchmarking for filehasher v2.
"""

import hashlib
import time
import os
from typing import Dict, List, Tuple, Optional
from enum import Enum
from dataclasses import dataclass
import socket
import json


class HashAlgorithm(Enum):
    """Supported hash algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"


@dataclass
class BenchmarkResult:
    """Results from algorithm benchmarking."""
    algorithm: str
    avg_time: float
    min_time: float
    max_time: float
    throughput_mbps: float


class HashCalculator:
    """Handles hash calculation for different algorithms."""
    
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.MD5, worker_id: int = 0, notify_progress_chunks: int = 0, udp_progress_port: int = None):
        self.algorithm = algorithm
        self._hasher = self._create_hasher()
        self.worker_id = worker_id
        self.notify_progress_chunks = notify_progress_chunks
        self.udp_progress_port = udp_progress_port
    
    def _create_hasher(self):
        """Create a new hasher instance for the current algorithm."""
        if self.algorithm == HashAlgorithm.MD5:
            return hashlib.md5()
        elif self.algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1()
        elif self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256()
        elif self.algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512()
        elif self.algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b()
        elif self.algorithm == HashAlgorithm.BLAKE2S:
            return hashlib.blake2s()
        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")

    def update_progress(self, file_path: str, files_processed: int, bytes_processed: int, file_size: int):
        """
        Update progress of the hash calculation.
        """
        if self.udp_progress_port is None:
            return
            
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            message = {
                'worker_id': self.worker_id,
                'bytes_processed': bytes_processed,
                'current_file': file_path,
                'files_processed': files_processed,
                'message_type': 'progress'
            }
            sock.sendto(json.dumps(message).encode('utf-8'), ('localhost', self.udp_progress_port))
            sock.close()
        except:
            pass  # Ignore UDP errors

    def calculate_file_hash(self, file_path: str, files_processed: int, chunk_size: int = 8192) -> str:
        """
        Calculate hash of a file using the specified algorithm.
        
        Args:
            file_path: Path to the file to hash
            chunk_size: Size of chunks to read at a time
            
        Returns:
            Hexadecimal hash string
        """
        hasher = self._create_hasher()
        
        bytes_processed = 0
        file_size = os.path.getsize(file_path)
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
                    bytes_processed += len(chunk)
                    if self.notify_progress_chunks > 0 and bytes_processed % (self.notify_progress_chunks * chunk_size) == 0:
                        self.update_progress(file_path, files_processed, bytes_processed, file_size)
            return hasher.hexdigest()
        except (IOError, OSError) as e:
            raise RuntimeError(f"Error reading file {file_path}: {e}")
    
    def calculate_metadata_hash(self, filename: str, size: int, mtime: int) -> str:
        """
        Calculate hash of file metadata for change detection.
        
        Args:
            filename: Name of the file
            size: File size in bytes
            mtime: Last modification time
            
        Returns:
            Hexadecimal hash string
        """
        metadata = f"{filename}|{size}|{mtime}"
        hasher = self._create_hasher()
        hasher.update(metadata.encode('utf-8'))
        return hasher.hexdigest()
    
    def format_hash(self, hash_value: str) -> str:
        """
        Format hash value with algorithm prefix if not MD5.
        
        Args:
            hash_value: Raw hash value
            
        Returns:
            Formatted hash string with algorithm prefix
        """
        if self.algorithm == HashAlgorithm.MD5:
            return hash_value  # No prefix for backwards compatibility
        else:
            return f"{self.algorithm.value.upper()}:{hash_value}"


class BenchmarkRunner:
    """Handles benchmarking of different hash algorithms."""
    
    def __init__(self, test_file_path: Optional[str] = None, iterations: int = 3):
        self.test_file_path = test_file_path
        self.iterations = iterations
    
    def create_test_file(self, size_mb: int = 10) -> str:
        """
        Create a temporary test file for benchmarking.
        
        Args:
            size_mb: Size of test file in megabytes
            
        Returns:
            Path to the created test file
        """
        import tempfile
        
        test_file = tempfile.NamedTemporaryFile(delete=False, suffix='.benchmark')
        size_bytes = size_mb * 1024 * 1024
        
        # Write random data in chunks
        chunk_size = 8192
        written = 0
        
        with open(test_file.name, 'wb') as f:
            while written < size_bytes:
                chunk = os.urandom(min(chunk_size, size_bytes - written))
                f.write(chunk)
                written += len(chunk)
        
        return test_file.name
    
    def benchmark_algorithm(self, algorithm: HashAlgorithm, test_file: str) -> BenchmarkResult:
        """
        Benchmark a single algorithm.
        
        Args:
            algorithm: Algorithm to benchmark
            test_file: Path to test file
            
        Returns:
            Benchmark results
        """
        calculator = HashCalculator(algorithm)
        times = []
        
        for _ in range(self.iterations):
            start_time = time.time()
            calculator.calculate_file_hash(test_file)
            end_time = time.time()
            times.append(end_time - start_time)
        
        file_size_mb = os.path.getsize(test_file) / (1024 * 1024)
        avg_time = sum(times) / len(times)
        throughput_mbps = file_size_mb / avg_time if avg_time > 0 else 0
        
        return BenchmarkResult(
            algorithm=algorithm.value,
            avg_time=avg_time,
            min_time=min(times),
            max_time=max(times),
            throughput_mbps=throughput_mbps
        )
    
    def benchmark_all(self, test_file_size_mb: int = 10) -> List[BenchmarkResult]:
        """
        Benchmark all supported algorithms.
        
        Args:
            test_file_size_mb: Size of test file in megabytes
            
        Returns:
            List of benchmark results for all algorithms
        """
        # Create test file if not provided
        if self.test_file_path is None:
            test_file = self.create_test_file(test_file_size_mb)
            cleanup_test_file = True
        else:
            test_file = self.test_file_path
            cleanup_test_file = False
        
        try:
            results = []
            for algorithm in HashAlgorithm:
                result = self.benchmark_algorithm(algorithm, test_file)
                results.append(result)
            
            # Sort by throughput (descending)
            results.sort(key=lambda x: x.throughput_mbps, reverse=True)
            return results
        
        finally:
            if cleanup_test_file and os.path.exists(test_file):
                os.unlink(test_file)
    
    def print_benchmark_results(self, results: List[BenchmarkResult]):
        """
        Print benchmark results in a formatted table.
        
        Args:
            results: List of benchmark results
        """
        from rich.console import Console
        from rich.table import Table
        
        console = Console()
        table = Table(title="Hash Algorithm Benchmark Results")
        
        table.add_column("Algorithm", style="cyan")
        table.add_column("Avg Time (s)", justify="right", style="green")
        table.add_column("Min Time (s)", justify="right", style="green")
        table.add_column("Max Time (s)", justify="right", style="green")
        table.add_column("Throughput (MB/s)", justify="right", style="magenta")
        
        for result in results:
            table.add_row(
                result.algorithm.upper(),
                f"{result.avg_time:.4f}",
                f"{result.min_time:.4f}",
                f"{result.max_time:.4f}",
                f"{result.throughput_mbps:.2f}"
            )
        
        console.print(table)


def get_algorithm_from_string(algorithm_str: str) -> HashAlgorithm:
    """
    Get HashAlgorithm enum from string.
    
    Args:
        algorithm_str: String representation of algorithm
        
    Returns:
        HashAlgorithm enum value
        
    Raises:
        ValueError: If algorithm string is not supported
    """
    algorithm_str = algorithm_str.lower().replace('-', '').replace('_', '')
    
    for algorithm in HashAlgorithm:
        if algorithm.value == algorithm_str:
            return algorithm
    
    raise ValueError(f"Unsupported algorithm: {algorithm_str}. "
                    f"Supported algorithms: {[alg.value for alg in HashAlgorithm]}")


def get_supported_algorithms() -> List[str]:
    """
    Get list of supported algorithm names.
    
    Returns:
        List of supported algorithm names
    """
    return [alg.value for alg in HashAlgorithm]
