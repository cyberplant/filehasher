"""
Multiprocessing file hashing with load balancing and progress reporting
"""

import multiprocessing as mp
from multiprocessing import Process, Queue, Event
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import time
import signal
import logging
from datetime import datetime
from dataclasses import dataclass

from rich.console import Console
from rich.table import Table

try:
    from .file_scanner import FileInfo
    from .hash_algorithms import HashAlgorithm, HashProcessor
except ImportError:
    from file_scanner import FileInfo
    from hash_algorithms import HashAlgorithm, HashProcessor


@dataclass
class HashResult:
    """Result of hashing a single file"""
    file_info: FileInfo
    primary_hash: str
    secondary_hash: str
    processing_time: float
    bytes_processed: int
    success: bool
    error: Optional[str] = None


@dataclass
class WorkerStats:
    """Statistics for a worker process"""
    worker_id: int
    files_processed: int
    bytes_processed: int
    total_time: float
    current_file: Optional[str] = None
    error_count: int = 0


class ProgressQueue:
    """Simple queue for progress updates"""
    
    def __init__(self):
        self._queue = Queue()
    
    def put(self, item):
        """Put progress update"""
        self._queue.put(item)
    
    def get_latest(self, timeout=None):
        """Get the latest progress update"""
        try:
            return self._queue.get(timeout=timeout)
        except:
            return None
    
    def clear(self):
        """Clear all pending updates"""
        while True:
            try:
                self._queue.get_nowait()
            except:
                break


def worker_process(worker_id: int, files: List[FileInfo], algorithm: HashAlgorithm,
                  progress_queue: Queue, stop_event: Event, results_queue: Queue):
    """
    Worker process function for hashing files
    
    Args:
        worker_id: Unique identifier for this worker
        files: List of files to process
        algorithm: Hash algorithm to use
        progress_queue: Queue for progress updates
        stop_event: Event to signal stopping
        results_queue: Queue for results
    """
    processor = HashProcessor(algorithm)
    stats = WorkerStats(worker_id=worker_id, files_processed=0, bytes_processed=0, total_time=0)
    start_time = time.time()
    
    # Process files (or skip if no files)
    if not files:
        # No files to process, send completion immediately
        stats.total_time = time.time() - start_time
        results_queue.put({
            'worker_id': worker_id,
            'completed': True,
            'stats': stats
        })
        return
    
    for file_info in files:
        if stop_event.is_set():
            break
        
        try:
            # Update progress
            progress_queue.put({
                'worker_id': worker_id,
                'current_file': str(file_info.relative_path),
                'files_processed': stats.files_processed,
                'total_files': len(files),
                'bytes_processed': stats.bytes_processed
            })
            
            # Process file
            file_start = time.time()
            
            def progress_callback(bytes_processed, total_bytes):
                if stop_event.is_set():
                    raise KeyboardInterrupt("Stopped by user")
            
            primary_hash = processor.compute_file_hash(file_info.path, progress_callback)
            
            # Compute secondary hash (MD5 for compatibility)
            if algorithm != HashAlgorithm.MD5:
                md5_processor = HashProcessor(HashAlgorithm.MD5)
                secondary_hash = md5_processor.compute_file_hash(file_info.path)
            else:
                secondary_hash = primary_hash
            
            processing_time = time.time() - file_start
            
            # Update stats
            stats.files_processed += 1
            stats.bytes_processed += file_info.size
            stats.total_time += processing_time
            
            # Send result
            result = HashResult(
                file_info=file_info,
                primary_hash=primary_hash,
                secondary_hash=secondary_hash,
                processing_time=processing_time,
                bytes_processed=file_info.size,
                success=True
            )
            results_queue.put(result)
            
        except Exception as e:
            stats.error_count += 1
            result = HashResult(
                file_info=file_info,
                primary_hash="",
                secondary_hash="",
                processing_time=time.time() - file_start,
                bytes_processed=0,
                success=False,
                error=str(e)
            )
            results_queue.put(result)
            logging.error(f"Worker {worker_id} error processing {file_info.path}: {e}")
    
    # Final progress update
    stats.total_time = time.time() - start_time
    results_queue.put({
        'worker_id': worker_id,
        'completed': True,
        'stats': stats
    })


class MultiprocessHashProcessor:
    """Handles multiprocess file hashing with progress reporting"""
    
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.MD5, 
                 num_workers: Optional[int] = None, quiet: bool = False):
        self.algorithm = algorithm
        self.num_workers = num_workers or mp.cpu_count()
        self.quiet = quiet
        self.console = Console()
        
        # Progress tracking
        self.progress_queues = []
        self.results_queue = Queue()
        self.stop_event = Event()
        self.workers = []
        
        # Results
        self.results: List[HashResult] = []
        self.worker_stats: List[WorkerStats] = []
        
        # Signal handling will be managed by the CLI
    
    def process_files(self, file_lists: List[List[FileInfo]]) -> List[HashResult]:
        """
        Process files using multiple workers
        
        Args:
            file_lists: List of file lists, one per worker
        
        Returns:
            List of HashResult objects
        """
        if not file_lists:
            return []
        
        self.results.clear()
        self.worker_stats.clear()
        self.stop_event.clear()
        
        # Track total processing time
        start_time = time.time()
        
        # Create progress queues for each worker
        self.progress_queues = [Queue() for _ in range(len(file_lists))]
        
        # Start workers
        self.workers = []
        for i, files in enumerate(file_lists):
            # Always start worker, even if no files (it will send completion immediately)
            worker = Process(
                target=worker_process,
                args=(i, files, self.algorithm, self.progress_queues[i], 
                      self.stop_event, self.results_queue)
            )
            worker.start()
            self.workers.append(worker)
        
        if not self.workers:
            return []
        
        # Collect results and show simple progress
        total_files = sum(len(files) for files in file_lists)
        total_workers = len(self.workers)
        last_update = time.time()
        
        if not self.quiet:
            self.console.print(f"[blue]Processing {total_files} files with {total_workers} workers...[/blue]")
        
        # Calculate adaptive timeout based on file sizes and workers
        total_bytes = sum(sum(f.size for f in files) for files in file_lists)
        # Estimate: 100MB/s per worker, with minimum 10s timeout
        estimated_time = max(10.0, total_bytes / (100 * 1024 * 1024 * total_workers))
        # Cap at 5 minutes
        adaptive_timeout = min(estimated_time, 300.0)
        
        # Collect results with adaptive timeout
        timeout_start = time.time()
        timeout_duration = 300  # 5 minutes maximum timeout
        completed_workers = 0
        
        while completed_workers < total_workers and not self.stop_event.is_set():
            if time.time() - timeout_start > timeout_duration:
                self.console.print("[yellow]Timeout reached, stopping...[/yellow]")
                break
                
            try:
                result = self.results_queue.get(timeout=adaptive_timeout)
                if isinstance(result, HashResult):
                    self.results.append(result)
                    # Show progress every few seconds (or every few files for faster feedback)
                    if not self.quiet and (time.time() - last_update > 3.0 or len(self.results) % 5 == 0):
                        processed_bytes = sum(r.bytes_processed for r in self.results)
                        file_percentage = (len(self.results) / total_files) * 100
                        bytes_percentage = (processed_bytes / total_bytes) * 100
                        self.console.print(f"[blue]Processed {len(self.results)}/{total_files} files ({file_percentage:.1f}%), {processed_bytes:,}/{total_bytes:,} bytes ({bytes_percentage:.1f}%)[/blue]")
                        last_update = time.time()
                elif isinstance(result, dict) and result.get('completed'):
                    completed_workers += 1
                    if 'stats' in result:
                        self.worker_stats.append(result['stats'])
                        if not self.quiet:
                            self.console.print(f"[blue]Worker {result['stats'].worker_id} completed: {result['stats'].files_processed} files, {result['stats'].bytes_processed} bytes[/blue]")
            except Exception as e:
                # Check if workers are still alive
                alive_workers = [w for w in self.workers if w.is_alive()]
                if not alive_workers:
                    break
                # Only show timeout message if it's not just a normal timeout
                if not self.quiet and "Empty" not in str(e):
                    self.console.print(f"[yellow]Timeout waiting for results (workers still processing)...[/yellow]")
                continue
        
        # Force cleanup of workers
        for worker in self.workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=2.0)
        
        # Store the total processing time
        self.total_processing_time = time.time() - start_time
        
        if not self.quiet:
            processed_bytes = sum(r.bytes_processed for r in self.results)
            self.console.print(f"[green]Completed processing {len(self.results)}/{total_files} files (100.0%), {processed_bytes:,}/{total_bytes:,} bytes (100.0%)[/green]")
        
        return self.results
    
    
    def get_statistics(self) -> Dict:
        """Get processing statistics"""
        if not self.results:
            return {}
        
        total_files = len(self.results)
        successful_files = sum(1 for r in self.results if r.success)
        failed_files = total_files - successful_files
        total_bytes = sum(r.bytes_processed for r in self.results if r.success)
        # Use actual wall clock time instead of sum of individual processing times
        total_time = getattr(self, 'total_processing_time', 0.0)
        avg_speed = total_bytes / total_time if total_time > 0 else 0
        
        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'total_bytes': total_bytes,
            'total_time': total_time,
            'avg_speed_bytes_per_sec': avg_speed,
            'worker_stats': [
                {
                    'worker_id': stats.worker_id,
                    'files_processed': stats.files_processed,
                    'bytes_processed': stats.bytes_processed,
                    'total_time': stats.total_time,
                    'error_count': stats.error_count,
                    'avg_speed': stats.bytes_processed / stats.total_time if stats.total_time > 0 else 0
                }
                for stats in self.worker_stats
            ]
        }
    
    def display_statistics(self):
        """Display processing statistics"""
        stats = self.get_statistics()
        if not stats:
            return
        
        # Overall statistics
        overall_table = Table(title="Processing Statistics", show_header=True)
        overall_table.add_column("Metric", style="cyan")
        overall_table.add_column("Value", style="green")
        
        overall_table.add_row("Total Files", str(stats['total_files']))
        overall_table.add_row("Successful", str(stats['successful_files']))
        overall_table.add_row("Failed", str(stats['failed_files']))
        overall_table.add_row("Total Bytes", f"{stats['total_bytes']:,}")
        overall_table.add_row("Total Time", f"{stats['total_time']:.2f}s")
        overall_table.add_row("Avg Speed", f"{stats['avg_speed_bytes_per_sec'] / (1024*1024):.2f} MB/s")
        
        self.console.print(overall_table)
        
        # Per-worker statistics
        if stats['worker_stats']:
            worker_table = Table(title="Per-Worker Statistics", show_header=True)
            worker_table.add_column("Worker", style="cyan")
            worker_table.add_column("Files", style="blue")
            worker_table.add_column("Bytes", style="green")
            worker_table.add_column("Time", style="yellow")
            worker_table.add_column("Speed", style="red")
            worker_table.add_column("Errors", style="red")
            
            for worker_stat in stats['worker_stats']:
                worker_table.add_row(
                    f"Worker {worker_stat['worker_id']}",
                    str(worker_stat['files_processed']),
                    f"{worker_stat['bytes_processed']:,}",
                    f"{worker_stat['total_time']:.2f}s",
                    f"{worker_stat['avg_speed'] / (1024*1024):.2f} MB/s",
                    str(worker_stat['error_count'])
                )
            
            self.console.print(worker_table)
    
    def stop(self):
        """Stop processing"""
        self.stop_event.set()
        
        # Wait for workers to finish
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=2.0)
                if worker.is_alive():
                    worker.terminate()
                    worker.join()


def create_processor(algorithm: HashAlgorithm, num_workers: Optional[int] = None, 
                    quiet: bool = False) -> MultiprocessHashProcessor:
    """Factory function to create a hash processor"""
    return MultiprocessHashProcessor(algorithm, num_workers, quiet)
