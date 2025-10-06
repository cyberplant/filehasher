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
from concurrent.futures import ProcessPoolExecutor, as_completed
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


def process_files_batch(files: List[FileInfo], algorithm: HashAlgorithm) -> List[HashResult]:
    """
    Process a batch of files using a single worker process.
    This function is designed to be called by ProcessPoolExecutor.
    
    Args:
        files: List of FileInfo objects to process
        algorithm: Hash algorithm to use
    
    Returns:
        List of HashResult objects
    """
    results = []
    processor = HashProcessor(algorithm)
    
    for file_info in files:
        try:
            start_time = time.time()
            primary_hash = processor.compute_file_hash(file_info.path)
            secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
            secondary_hash = processor.compute_data_hash(secondary_data)
            
            result = HashResult(
                file_info=file_info,
                primary_hash=primary_hash,
                secondary_hash=secondary_hash,
                processing_time=time.time() - start_time,
                bytes_processed=file_info.size,
                success=True
            )
            results.append(result)
            
        except Exception as e:
            result = HashResult(
                file_info=file_info,
                primary_hash="",
                secondary_hash="",
                processing_time=time.time() - start_time,
                bytes_processed=0,
                success=False,
                error=str(e)
            )
            results.append(result)
            logging.error(f"Error processing {file_info.path}: {e}")
    
    return results


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
        # Limit queue size to prevent memory issues with large datasets
        self.results_queue = Queue(maxsize=1000)
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
        
        # Create progress queues for each worker with limited buffer size
        self.progress_queues = [Queue(maxsize=100) for _ in range(len(file_lists))]
        
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
        
        # Collect results with adaptive timeout and batch processing
        timeout_start = time.time()
        timeout_duration = 300  # 5 minutes maximum timeout
        completed_workers = 0
        results_batch_size = 1000  # Process results in batches to avoid queue overflow
        
        while completed_workers < total_workers and not self.stop_event.is_set():
            if time.time() - timeout_start > timeout_duration:
                self.console.print("[yellow]Timeout reached, stopping...[/yellow]")
                break
                
            try:
                result = self.results_queue.get(timeout=adaptive_timeout)
                if isinstance(result, HashResult):
                    self.results.append(result)
                    # Show progress less frequently for large datasets to reduce queue pressure
                    progress_interval = max(100, total_files // 100)  # Dynamic interval based on total files
                    if not self.quiet and (time.time() - last_update > 3.0 or len(self.results) % progress_interval == 0):
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
    
    def process_files_streaming(self, file_lists: List[List[FileInfo]], 
                               output_file: Path, hash_algorithm: HashAlgorithm) -> Dict:
        """
        Process files and write results directly to hash file in streaming fashion
        to avoid memory accumulation with large datasets.
        
        Args:
            file_lists: List of file lists, one per worker
            output_file: Path to output hash file
            hash_algorithm: Hash algorithm to use
        
        Returns:
            Dictionary with processing statistics
        """
        if not file_lists:
            return {}
        
        self.results.clear()
        self.worker_stats.clear()
        self.stop_event.clear()
        
        # Track total processing time
        start_time = time.time()
        
        # Create progress queues for each worker with limited buffer size
        self.progress_queues = [Queue(maxsize=100) for _ in range(len(file_lists))]
        
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
            return {}
        
        # Collect results and write to file in streaming fashion
        total_files = sum(len(files) for files in file_lists)
        total_workers = len(self.workers)
        total_bytes = sum(sum(f.size for f in files) for files in file_lists)
        last_update = time.time()
        
        if not self.quiet:
            self.console.print(f"[blue]Processing {total_files} files with {total_workers} workers (streaming mode)...[/blue]")
        
        # Use shorter timeout for large datasets to prevent queue overflow
        adaptive_timeout = 5.0  # 5 seconds timeout for queue operations
        
        # Stream results to file
        timeout_start = time.time()
        timeout_duration = 300  # 5 minutes maximum timeout
        completed_workers = 0
        processed_count = 0
        processed_bytes = 0
        
        # Collect results in batches and write to file
        entries_batch = []
        batch_size = 500  # Write in smaller batches to avoid memory issues
        
        try:
            while completed_workers < total_workers and not self.stop_event.is_set():
                if time.time() - timeout_start > timeout_duration:
                    self.console.print("[yellow]Timeout reached, stopping...[/yellow]")
                    break
                    
                try:
                    result = self.results_queue.get(timeout=adaptive_timeout)
                    if isinstance(result, HashResult):
                        # Convert result to HashEntry
                        from .hash_file import HashEntry
                        entry = HashEntry(
                            primary_hash=result.primary_hash,
                            secondary_hash=result.secondary_hash,
                            directory=result.file_info.directory,
                            filename=result.file_info.filename,
                            size=result.file_info.size,
                            inode=result.file_info.inode,
                            mtime=result.file_info.mtime,
                            is_symlink=result.file_info.is_symlink
                        )
                        entries_batch.append(entry)
                        
                        processed_count += 1
                        processed_bytes += result.bytes_processed
                        
                        # Write batch when it reaches batch_size
                        if len(entries_batch) >= batch_size:
                            self._write_batch_to_file(output_file, entries_batch, hash_algorithm, processed_count == batch_size)
                            entries_batch.clear()
                        
                        # Show progress less frequently for large datasets
                        progress_interval = max(1000, total_files // 50)  # Dynamic interval
                        if not self.quiet and (time.time() - last_update > 5.0 or processed_count % progress_interval == 0):
                            file_percentage = (processed_count / total_files) * 100
                            bytes_percentage = (processed_bytes / total_bytes) * 100
                            self.console.print(f"[blue]Processed {processed_count}/{total_files} files ({file_percentage:.1f}%), {processed_bytes:,}/{total_bytes:,} bytes ({bytes_percentage:.1f}%)[/blue]")
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
        
        finally:
            # Write remaining entries
            if entries_batch:
                self._write_batch_to_file(output_file, entries_batch, hash_algorithm, False)
            
            # Force cleanup of workers
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
                worker.join(timeout=2.0)
        
        # Store the total processing time
        self.total_processing_time = time.time() - start_time
        
        if not self.quiet:
            self.console.print(f"[green]Completed processing {processed_count}/{total_files} files (100.0%), {processed_bytes:,}/{total_bytes:,} bytes (100.0%)[/green]")
        
        # Return statistics
        return {
            'total_files': processed_count,
            'successful': processed_count,  # All processed files are successful in streaming mode
            'failed': 0,
            'total_bytes': processed_bytes,
            'total_time': self.total_processing_time,
            'avg_speed': (processed_bytes / self.total_processing_time) / (1024 * 1024) if self.total_processing_time > 0 else 0,
            'worker_stats': self.worker_stats
        }
    
    
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
    
    def display_statistics_from_dict(self, stats: Dict):
        """Display statistics from a dictionary (for streaming mode)"""
        if not stats:
            return
            
        # Create main statistics table
        main_table = Table(title="Processing Statistics")
        main_table.add_column("Metric", style="cyan")
        main_table.add_column("Value", justify="right")
        
        main_table.add_row("Total Files", str(stats.get('total_files', 0)))
        main_table.add_row("Successful", str(stats.get('successful', 0)))
        main_table.add_row("Failed", str(stats.get('failed', 0)))
        main_table.add_row("Total Bytes", f"{stats.get('total_bytes', 0):,}")
        main_table.add_row("Total Time", f"{stats.get('total_time', 0):.2f}s")
        main_table.add_row("Avg Speed", f"{stats.get('avg_speed', 0):.2f} MB/s")
        
        self.console.print(main_table)
        
        # Display per-worker statistics if available
        worker_stats = stats.get('worker_stats', [])
        if worker_stats:
            self.console.print("\n")
            worker_table = Table(title="Per-Worker Statistics")
            worker_table.add_column("Worker", style="cyan")
            worker_table.add_column("Files", justify="right")
            worker_table.add_column("Bytes", justify="right")
            worker_table.add_column("Time", justify="right")
            worker_table.add_column("Speed", justify="right")
            worker_table.add_column("Errors", justify="right")
            
            for worker_stat in worker_stats:
                speed_mbps = (worker_stat.bytes_processed / worker_stat.total_time) / (1024 * 1024) if worker_stat.total_time > 0 else 0
                worker_table.add_row(
                    f"Worker {worker_stat.worker_id}",
                    str(worker_stat.files_processed),
                    f"{worker_stat.bytes_processed:,}",
                    f"{worker_stat.total_time:.2f}s",
                    f"{speed_mbps:.2f} MB/s",
                    str(worker_stat.errors)
                )
            
            self.console.print(worker_table)
    
    def process_files_with_executor(self, file_lists: List[List[FileInfo]]) -> List[HashResult]:
        """
        Process files using ProcessPoolExecutor for better scalability with large datasets.
        
        Args:
            file_lists: List of file lists, one per worker
        
        Returns:
            List of HashResult objects
        """
        if not file_lists:
            return []
        
        # Track total processing time
        start_time = time.time()
        
        # Calculate total files and bytes for progress reporting
        total_files = sum(len(files) for files in file_lists)
        total_bytes = sum(sum(f.size for f in files) for files in file_lists)
        
        if not self.quiet:
            self.console.print(f"[blue]Processing {total_files} files using ProcessPoolExecutor...[/blue]")
        
        all_results = []
        processed_count = 0
        processed_bytes = 0
        last_update = time.time()
        
        # Use ProcessPoolExecutor for better scalability
        with ProcessPoolExecutor(max_workers=len(file_lists)) as executor:
            # Submit all batches
            future_to_batch = {}
            for i, files in enumerate(file_lists):
                if files:  # Only submit non-empty batches
                    future = executor.submit(process_files_batch, files, self.algorithm)
                    future_to_batch[future] = (i, files)
            
            # Process completed batches as they finish
            for future in as_completed(future_to_batch):
                batch_id, batch_files = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    
                    # Update progress
                    batch_processed = len(batch_results)
                    batch_bytes = sum(r.bytes_processed for r in batch_results)
                    processed_count += batch_processed
                    processed_bytes += batch_bytes
                    
                    # Show progress updates
                    progress_interval = max(1000, total_files // 50)  # Dynamic interval
                    if not self.quiet and (time.time() - last_update > 2.0 or processed_count % progress_interval == 0):
                        file_percentage = (processed_count / total_files) * 100
                        bytes_percentage = (processed_bytes / total_bytes) * 100
                        self.console.print(f"[blue]Processed {processed_count}/{total_files} files ({file_percentage:.1f}%), {processed_bytes:,}/{total_bytes:,} bytes ({bytes_percentage:.1f}%)[/blue]")
                        last_update = time.time()
                        
                except Exception as e:
                    logging.error(f"Batch {batch_id} failed: {e}")
                    if not self.quiet:
                        self.console.print(f"[red]Batch {batch_id} failed: {e}[/red]")
        
        # Store the total processing time
        self.total_processing_time = time.time() - start_time
        
        if not self.quiet:
            self.console.print(f"[green]Completed processing {processed_count}/{total_files} files (100.0%), {processed_bytes:,}/{total_bytes:,} bytes (100.0%)[/green]")
        
        # Store results for statistics
        self.results = all_results
        
        return all_results
    
    def _write_batch_to_file(self, output_file: Path, entries_batch: List, hash_algorithm: HashAlgorithm, is_first_batch: bool):
        """Helper method to write a batch of entries to the hash file"""
        from .hash_file import HashFile
        
        # For the first batch, we need to determine the base directory
        # We'll use the parent directory of the output file as the base
        base_directory = output_file.parent
        
        hash_file = HashFile(output_file)
        
        if is_first_batch:
            # Create new file with header
            hash_file.write(entries_batch, base_directory, hash_algorithm, update_mode=False)
        else:
            # Append to existing file
            hash_file.write(entries_batch, base_directory, hash_algorithm, update_mode=True)
    
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
