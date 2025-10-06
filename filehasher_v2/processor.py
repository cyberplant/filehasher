"""
Multiprocessing processor with pipe communication for file hashing.
"""

import os
import time
import signal
import multiprocessing
import socket
import threading
import json
import random
from typing import List, Dict, Any, Optional, Tuple
from multiprocessing import Process, Pipe
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .file_scanner import FileInfo, FileScanner
from .hash_algorithms import HashCalculator, HashAlgorithm
from .hash_file import HashFileWriter, HashEntry


@dataclass
class ProgressUpdate:
    """Progress update from worker process."""
    worker_id: int
    files_processed: int
    bytes_processed: int
    current_file: str
    total_files: int
    total_bytes: int


@dataclass
class HashResult:
    """Hash result from worker process."""
    worker_id: int
    file_info: FileInfo
    metadata_hash: str
    file_hash: str


@dataclass
class WorkerStats:
    """Statistics for a worker process."""
    worker_id: int
    files_processed: int
    bytes_processed: int
    processing_time: float
    throughput_mbps: float


@dataclass
class UDPProgressMessage:
    """UDP progress message from worker."""
    worker_id: int
    files_processed: int
    bytes_processed: int
    current_file: str
    message_type: str  # 'progress' or 'complete'


class UDPProgressListener:
    """UDP listener for progress updates from workers."""
    
    def __init__(self, port: int = 0):
        self.port = port
        self.sock = None
        self.listening = False
        self.worker_stats = {}
        self.total_files_processed = 0
        self.total_bytes_processed = 0
        self.lock = threading.Lock()
    
    def start(self):
        """Start UDP listener."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', self.port))
        self.port = self.sock.getsockname()[1]  # Get actual port
        self.listening = True
        
        # Start listener thread
        self.listener_thread = threading.Thread(target=self._listen, daemon=True)
        self.listener_thread.start()
    
    def stop(self):
        """Stop UDP listener."""
        self.listening = False
        if self.sock:
            self.sock.close()
    
    def _listen(self):
        """Listen for UDP messages."""
        while self.listening:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                self._handle_message(message)
            except (socket.error, json.JSONDecodeError):
                continue
    
    def _handle_message(self, message: dict):
        """Handle incoming progress message."""
        with self.lock:
            worker_id = message['worker_id']
            
            if worker_id not in self.worker_stats:
                self.worker_stats[worker_id] = {
                    'files_processed': 0,
                    'bytes_processed': 0,
                    'start_time': time.time()
                }
            
            if message['message_type'] == 'progress':
                self.worker_stats[worker_id]['files_processed'] = message['files_processed']
                self.worker_stats[worker_id]['bytes_processed'] = message['bytes_processed']
                
                # Update totals
                self.total_files_processed = sum(ws['files_processed'] for ws in self.worker_stats.values())
                self.total_bytes_processed = sum(ws['bytes_processed'] for ws in self.worker_stats.values())
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress."""
        with self.lock:
            return {
                'files_processed': self.total_files_processed,
                'bytes_processed': self.total_bytes_processed,
                'worker_stats': self.worker_stats.copy()
            }


class HashProcessor:
    """Main processor for handling multiprocessing file hashing using ProcessPoolExecutor."""
    
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.MD5, num_workers: Optional[int] = None):
        self.algorithm = algorithm
        self.num_workers = num_workers or multiprocessing.cpu_count()
        self._executor: Optional[ProcessPoolExecutor] = None
        self._results: List[HashResult] = []
        self._worker_stats: List[WorkerStats] = []
        self._interrupted = False
        self._udp_listener: Optional[UDPProgressListener] = None
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals for graceful shutdown."""
        self._interrupted = True
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self._cleanup_processes()
    
    def process_directory(self, directory: str, output_path: str, follow_symlinks: bool = False, 
                         quiet: bool = False) -> bool:
        """
        Process a directory and generate hash file using ProcessPoolExecutor.
        
        Args:
            directory: Directory to process
            output_path: Output hash file path
            follow_symlinks: Whether to follow symbolic links
            quiet: Whether to suppress progress output
            
        Returns:
            True if successful, False if interrupted or failed
        """
        try:
            # Scan directory for files
            if not quiet:
                print(f"Scanning directory: {directory}")
            
            scanner = FileScanner(directory, follow_symlinks)
            files, symlinks = scanner.scan_directory()
            
            if not files:
                print("No files found to process.")
                return True
            
            if not quiet:
                print(f"Found {len(files)} files ({scanner.get_total_size():,} bytes)")
                if symlinks:
                    print(f"Found {len(symlinks)} symlinks")
            
            # Distribute files across workers
            worker_files = scanner.distribute_files(self.num_workers)
            
            if not quiet:
                scanner.print_distribution_summary(worker_files)
            
            # Initialize hash file with headers
            self._initialize_hash_file(output_path, directory)
            
            # Process files using ProcessPoolExecutor
            success = self._process_files_with_processpool(worker_files, output_path, symlinks, quiet)
            
            if success and not quiet:
                self._print_final_stats()
            
            return success
        
        except Exception as e:
            print(f"Error processing directory: {e}")
            return False
        
        finally:
            self._cleanup_executor()
    
    def _process_files_with_processpool(self, worker_files: List[List[FileInfo]], output_path: str, 
                                       symlinks: List[FileInfo], quiet: bool) -> bool:
        """
        Process files using ProcessPoolExecutor with progress communication.
        
        Args:
            worker_files: List of file lists per worker
            output_path: Path to hash file
            symlinks: List of symlink entries
            quiet: Whether to suppress progress output
            
        Returns:
            True if successful, False if interrupted
        """
        self._results.clear()
        self._worker_stats.clear()
        
        start_time = time.time()
        last_update = start_time
        update_interval = 1.0  # Update every second
        
        # Flatten all files for processing
        all_files = []
        for files in worker_files:
            all_files.extend(files)
        
        if not all_files:
            return True
        
        # Create ProcessPoolExecutor
        self._executor = ProcessPoolExecutor(max_workers=self.num_workers)
        
        # Start UDP progress listener
        self._udp_listener = UDPProgressListener()
        self._udp_listener.start()
        
        # Create progress tracking
        progress_tracker = {
            'files_processed': 0,
            'bytes_processed': 0,
            'total_files': len(all_files),
            'total_bytes': sum(f.size for f in all_files)
        }
        
        try:
            # Distribute files in batches to workers
#            files_per_worker = max(1, len(all_files) // self.num_workers)
#            file_batches = []
#            
#            for i in range(0, len(all_files), files_per_worker):
#                batch = all_files[i:i + files_per_worker]
#                if batch:  # Only add non-empty batches
#                    file_batches.append(batch)
#            
            # Submit batches for processing
            future_to_batch = {}
            
            for worker_id, batch in enumerate(worker_files):
                future = self._executor.submit(
                    process_file_batch_with_udp, 
                    worker_id,
                    batch, 
                    self.algorithm,
                    self._udp_listener.port
                )
                future_to_batch[future] = batch
            
            # Open hash file for appending
            with open(output_path, 'a', encoding='utf-8') as hash_file:
                # Process completed futures
                for future in as_completed(future_to_batch):
                    if self._interrupted:
                        return False
                    
                    try:
                        results = future.result()
                        if results:
                            # Write all hash results from this batch
                            for result in results:
                                self._write_hash_entry(hash_file, result)
                                self._results.append(result)
                            
                            # Show progress update if not quiet
                            current_time = time.time()
                            if not quiet and current_time - last_update >= update_interval:
                                # Get progress from UDP listener
                                progress = self._udp_listener.get_progress()
                                self._show_progress_update(progress, progress_tracker)
                                last_update = current_time
                    
                    except Exception as e:
                        batch = future_to_batch[future]
                        print(f"Error processing batch of {len(batch)} files: {e}")
                        print(e)
                        continue
                
                # Write symlink entries
                for symlink in symlinks:
                    self._write_symlink_entry(hash_file, symlink)
            
            # Calculate final statistics from UDP listener
            processing_time = time.time() - start_time
            self._worker_stats = []
            
            # Get final worker stats from UDP listener
            final_progress = self._udp_listener.get_progress()
            for worker_id, stats in final_progress['worker_stats'].items():
                if stats['files_processed'] > 0:
                    worker_time = processing_time  # All workers run in parallel
                    
                    self._worker_stats.append(WorkerStats(
                        worker_id=worker_id + 1,
                        files_processed=stats['files_processed'],
                        bytes_processed=stats['bytes_processed'],
                        processing_time=worker_time,
                        throughput_mbps=stats['bytes_processed'] / worker_time if worker_time > 0 else 0
                    ))
            
            return True
        
        except Exception as e:
            print(f"Error in ProcessPoolExecutor: {e}")
            return False
    
    def _initialize_hash_file(self, output_path: str, base_directory: str):
        """Initialize hash file with metadata headers."""
        from .hash_file import HashFileWriter
        
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Write headers only
        writer = HashFileWriter(output_path, self.algorithm)
        writer.write_file(base_directory)
    
    def _show_progress_update(self, progress: Dict[str, Any], progress_tracker: Dict[str, Any]):
        """Show progress update."""
        files_processed = progress['files_processed']
        bytes_processed = progress['bytes_processed']
        total_files = progress_tracker['total_files']
        total_bytes = progress_tracker['total_bytes']
        
        # Calculate percentage
        file_percent = (files_processed / total_files * 100) if total_files > 0 else 0
        byte_percent = (bytes_processed / total_bytes * 100) if total_bytes > 0 else 0
        
        print(f"\rProcessed {files_processed}/{total_files} files ({file_percent:.1f}%), "
              f"{self._format_bytes(bytes_processed)}/{self._format_bytes(total_bytes)} ({byte_percent:.1f}%)", 
              end='', flush=True)
    
    
    
    def _write_hash_entry(self, hash_file, result: HashResult):
        """Write a single hash entry to the file."""
        # Split relative path into directory and filename
        relative_path = Path(result.file_info.relative_path)
        directory = str(relative_path.parent) if relative_path.parent != Path('.') else '.'
        filename = relative_path.name
        
        # Format file hash with algorithm prefix if needed
        formatted_hash = result.file_hash
        if self.algorithm != HashAlgorithm.MD5:
            formatted_hash = f"{self.algorithm.value.upper()}:{result.file_hash}"
        
        # Write hash entry
        hash_file.write(f"{result.metadata_hash}|{formatted_hash}|{directory}|{filename}|{result.file_info.size}|{result.file_info.inode}|{result.file_info.mtime}\n")
        hash_file.flush()  # Ensure immediate write to disk
    
    def _write_symlink_entry(self, hash_file, symlink: FileInfo):
        """Write a symlink entry to the file."""
        # Split relative path into directory and filename
        relative_path = Path(symlink.relative_path)
        directory = str(relative_path.parent) if relative_path.parent != Path('.') else '.'
        filename = relative_path.name
        
        # Write symlink as commented line
        hash_file.write(f"# SYMLINK|{directory}|{filename}|{symlink.size}|{symlink.inode}|{symlink.mtime}\n")
        hash_file.flush()  # Ensure immediate write to disk
    
    def _print_final_stats(self):
        """Print final processing statistics."""
        if not self._worker_stats:
            return
        
        from rich.console import Console
        from rich.table import Table
        
        console = Console()
        
        # Overall statistics
        total_time = max(stat.processing_time for stat in self._worker_stats)
        total_files = sum(stat.files_processed for stat in self._worker_stats)
        total_bytes = sum(stat.bytes_processed for stat in self._worker_stats)
        overall_speed = total_bytes / total_time if total_time > 0 else 0
        
        # Overall stats table
        overall_table = Table(title="Performance Statistics")
        overall_table.add_column("Metric", style="cyan")
        overall_table.add_column("Value", justify="right", style="green")
        
        overall_table.add_row("Total Time", f"{total_time:.2f} seconds")
        overall_table.add_row("Total Files", str(total_files))
        overall_table.add_row("Total Bytes", self._format_bytes(total_bytes))
        overall_table.add_row("Overall Speed", f"{self._format_bytes(int(overall_speed))}/sec")
        
        console.print(overall_table)
        
        # Per-worker statistics
        worker_table = Table(title="Per-Thread Statistics")
        worker_table.add_column("Thread", style="cyan")
        worker_table.add_column("Files", justify="right", style="green")
        worker_table.add_column("Bytes", justify="right", style="blue")
        worker_table.add_column("Time", justify="right", style="yellow")
        worker_table.add_column("Speed", justify="right", style="magenta")
        
        for stat in self._worker_stats:
            worker_table.add_row(
                f"Thread {stat.worker_id + 1}",
                str(stat.files_processed),
                self._format_bytes(stat.bytes_processed),
                f"{stat.processing_time:.2f}s",
                f"{self._format_bytes(int(stat.throughput_mbps))}/sec"
            )
        
        console.print(worker_table)
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string."""
        if bytes_value == 0:
            return "0 bytes"
        
        units = ['bytes', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(bytes_value)
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.1f} {units[unit_index]}"
    
    def _cleanup_executor(self):
        """Clean up ProcessPoolExecutor and UDP listener."""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        
        if self._udp_listener:
            self._udp_listener.stop()
            self._udp_listener = None


def process_single_file(file_info: FileInfo, algorithm: HashAlgorithm) -> Optional[HashResult]:
    """
    Process a single file and return hash result.
    
    Args:
        file_info: File information
        algorithm: Hash algorithm to use
        
    Returns:
        HashResult if successful, None if failed
    """
    try:
        calculator = HashCalculator(algorithm)
        
        # Calculate file hash
        file_hash = calculator.calculate_file_hash(file_info.path)
        
        # Calculate metadata hash
        filename = Path(file_info.relative_path).name
        metadata_hash = calculator.calculate_metadata_hash(
            filename, file_info.size, file_info.mtime
        )
        
        return HashResult(
            worker_id=0,  # Not used in ProcessPoolExecutor
            file_info=file_info,
            metadata_hash=metadata_hash,
            file_hash=file_hash
        )
        
    except Exception as e:
        print(f"Error processing {file_info.path}: {e}")
        return None


def process_single_file_with_progress(file_info: FileInfo, algorithm: HashAlgorithm) -> Optional[HashResult]:
    """
    Process a single file with progress communication.
    
    Args:
        file_info: File information
        algorithm: Hash algorithm to use
        
    Returns:
        HashResult if successful, None if failed
    """
    try:
        calculator = HashCalculator(algorithm)
        
        # Calculate file hash
        file_hash = calculator.calculate_file_hash(file_info.path)
        
        # Calculate metadata hash
        filename = Path(file_info.relative_path).name
        metadata_hash = calculator.calculate_metadata_hash(
            filename, file_info.size, file_info.mtime
        )
        
        return HashResult(
            worker_id=0,  # Not used in ProcessPoolExecutor
            file_info=file_info,
            metadata_hash=metadata_hash,
            file_hash=file_hash
        )
        
    except Exception as e:
        print(f"Error processing {file_info.path}: {e}")
        return None


def process_single_file_with_worker_id(file_info: FileInfo, algorithm: HashAlgorithm, worker_id: int) -> Optional[HashResult]:
    """
    Process a single file with assigned worker ID.
    
    Args:
        file_info: File information
        algorithm: Hash algorithm to use
        worker_id: Assigned worker ID
        
    Returns:
        HashResult if successful, None if failed
    """
    try:
        calculator = HashCalculator(algorithm)
        
        # Calculate file hash
        file_hash = calculator.calculate_file_hash(file_info.path)
        
        # Calculate metadata hash
        filename = Path(file_info.relative_path).name
        metadata_hash = calculator.calculate_metadata_hash(
            filename, file_info.size, file_info.mtime
        )
        
        return HashResult(
            worker_id=worker_id,
            file_info=file_info,
            metadata_hash=metadata_hash,
            file_hash=file_hash
        )
        
    except Exception as e:
        print(f"Error processing {file_info.path}: {e}")
        return None


def process_single_file_with_udp(file_info: FileInfo, algorithm: HashAlgorithm, udp_port: int) -> Optional[HashResult]:
    """
    Process a single file with UDP progress updates.
    
    Args:
        file_info: File information
        algorithm: Hash algorithm to use
        udp_port: UDP port for progress updates
        
    Returns:
        HashResult if successful, None if failed
    """
    try:
        # Generate a unique worker ID for this process
        worker_id = random.randint(1000, 9999)
        
        # Send progress update via UDP
        def send_progress(files_processed: int, bytes_processed: int):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                message = {
                    'worker_id': worker_id,
                    'files_processed': files_processed,
                    'bytes_processed': bytes_processed,
                    'current_file': Path(file_info.relative_path).name,
                    'message_type': 'progress'
                }
                sock.sendto(json.dumps(message).encode('utf-8'), ('localhost', udp_port))
                sock.close()
            except:
                pass  # Ignore UDP errors
        
        calculator = HashCalculator(algorithm)
        
        # Calculate file hash
        file_hash = calculator.calculate_file_hash(file_info.path)
        
        # Calculate metadata hash
        filename = Path(file_info.relative_path).name
        metadata_hash = calculator.calculate_metadata_hash(
            filename, file_info.size, file_info.mtime
        )
        
        # Send progress update
        send_progress(1, file_info.size)
        
        return HashResult(
            worker_id=worker_id,
            file_info=file_info,
            metadata_hash=metadata_hash,
            file_hash=file_hash
        )
        
    except Exception as e:
        print(f"Error processing {file_info.path}: {e}")
        return None


def process_file_batch_with_udp(worker_id: int, file_batch: List[FileInfo], algorithm: HashAlgorithm, udp_port: int) -> List[HashResult]:
    """
    Process a batch of files with UDP progress updates.
    
    Args:
        file_batch: List of file information
        algorithm: Hash algorithm to use
        udp_port: UDP port for progress updates
        
    Returns:
        List of HashResult objects
    """
    try:
        # Send progress update via UDP
        def send_progress(files_processed: int, bytes_processed: int, current_file: str = ""):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                message = {
                    'worker_id': worker_id,
                    'files_processed': files_processed,
                    'bytes_processed': bytes_processed,
                    'current_file': current_file,
                    'message_type': 'progress'
                }
                sock.sendto(json.dumps(message).encode('utf-8'), ('localhost', udp_port))
                sock.close()
            except:
                pass  # Ignore UDP errors
        
        calculator = HashCalculator(algorithm)
        results = []
        files_processed = 0
        bytes_processed = 0
        
        for file_info in file_batch:
            try:
                # Calculate file hash
                file_hash = calculator.calculate_file_hash(file_info.path)
                
                # Calculate metadata hash
                filename = Path(file_info.relative_path).name
                metadata_hash = calculator.calculate_metadata_hash(
                    filename, file_info.size, file_info.mtime
                )
                
                # Create result
                result = HashResult(
                    worker_id=worker_id,
                    file_info=file_info,
                    metadata_hash=metadata_hash,
                    file_hash=file_hash
                )
                results.append(result)
                
                # Update counters
                files_processed += 1
                bytes_processed += file_info.size
                
                # Send progress update every few files or for the last file
                if files_processed % 10 == 0 or files_processed == len(file_batch):
                    send_progress(files_processed, bytes_processed, filename)
                
            except Exception as e:
                print(f"Error processing {file_info.path}: {e}")
                continue
        
        # Send final progress update
        send_progress(files_processed, bytes_processed)
        
        return results
        
    except Exception as e:
        print(f"Error processing batch of {len(file_batch)} files: {e}")
        print(e)
        raise e
        return []


def process_files_batch(files: List[FileInfo], algorithm: HashAlgorithm, worker_id: int = 0) -> Dict[str, Any]:
    """
    Process a batch of files (alternative single-threaded function).
    
    Args:
        files: List of files to process
        algorithm: Hash algorithm to use
        worker_id: ID of this worker
        
    Returns:
        Dictionary with processing results
    """
    calculator = HashCalculator(algorithm)
    results = []
    files_processed = 0
    bytes_processed = 0
    start_time = time.time()
    
    for file_info in files:
        try:
            # Calculate file hash
            file_hash = calculator.calculate_file_hash(file_info.path)
            
            # Calculate metadata hash
            filename = Path(file_info.relative_path).name
            metadata_hash = calculator.calculate_metadata_hash(
                filename, file_info.size, file_info.mtime
            )
            
            results.append({
                'file_info': file_info,
                'metadata_hash': metadata_hash,
                'file_hash': file_hash
            })
            
            files_processed += 1
            bytes_processed += file_info.size
            
        except Exception as e:
            print(f"Error processing {file_info.path}: {e}")
            continue
    
    processing_time = time.time() - start_time
    throughput_mbps = bytes_processed / processing_time if processing_time > 0 else 0
    
    return {
        'results': results,
        'files_processed': files_processed,
        'bytes_processed': bytes_processed,
        'processing_time': processing_time,
        'throughput_mbps': throughput_mbps
    }