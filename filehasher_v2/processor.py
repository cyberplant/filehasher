"""
Multiprocessing file hashing with progress reporting
"""

import multiprocessing as mp
from multiprocessing import Process, Queue
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import signal
import threading

from rich.console import Console
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.live import Live
from rich.table import Table

from .file_scanner import FileInfo
from .hash_algorithms import HashAlgorithm, HashError
from .hash_file import HashEntry


class ProcessingResult:
    """Result of file processing"""
    def __init__(self, file_info: FileInfo, file_hash: str = None, other_hash: str = None, error: str = None):
        self.file_info = file_info
        self.file_hash = file_hash
        self.other_hash = other_hash
        self.error = error
        self.success = error is None


def process_file_worker(file_info: FileInfo, algorithm_name: str, result_queue: Queue, progress_queue: Queue, thread_id: int = 0):
    """Worker function to process a single file"""
    from .hash_algorithms import get_algorithm
    
    def progress_callback(bytes_processed, file_size):
        """Progress callback for file hashing"""
        progress_queue.put({
            'thread_id': thread_id,
            'filename': file_info.relative_path.name,
            'size': file_info.size,
            'file_progress': bytes_processed,
            'file_total': file_size,
            'type': 'file_progress'
        })
    
    try:
        # Send start notification
        progress_queue.put({
            'thread_id': thread_id,
            'filename': file_info.relative_path.name,
            'size': file_info.size,
            'type': 'file_start'
        })
        
        # Get algorithm in worker process
        algorithm = get_algorithm(algorithm_name)
        
        # Hash the file with progress callback
        file_hash = algorithm.hash_file(file_info.path, progress_callback=progress_callback)
        
        # For now, use a simple secondary hash (file size + mtime)
        # This could be enhanced with a different algorithm
        secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
        other_hash = algorithm.hash_data(secondary_data)
        
        result = ProcessingResult(file_info, file_hash, other_hash)
        result_queue.put(result)
        
        # Send completion notification
        progress_queue.put({
            'thread_id': thread_id,
            'filename': file_info.relative_path.name,
            'size': file_info.size,
            'success': True,
            'type': 'file_complete'
        })
        
    except HashError as e:
        result = ProcessingResult(file_info, error=str(e))
        result_queue.put(result)
        
        # Send error notification
        progress_queue.put({
            'thread_id': thread_id,
            'filename': file_info.relative_path.name,
            'size': file_info.size,
            'success': False,
            'error': str(e),
            'type': 'file_complete'
        })


class FileProcessor:
    """Handles multiprocessing file hashing with progress reporting"""
    
    def __init__(self, algorithm: HashAlgorithm, num_processes: int = 1, show_progress: bool = True):
        self.algorithm = algorithm
        self.num_processes = num_processes
        self.show_progress = show_progress
        self.should_stop = False
        # Don't create console here - create it when needed to avoid pickle issues
    
    def process_files(self, file_batches: List[List[FileInfo]]) -> List[ProcessingResult]:
        """Process files using multiprocessing with progress reporting"""
        if not file_batches:
            return []
        
        if self.num_processes == 1:
            # Single process mode
            return self._process_files_single(file_batches[0])
        
        # Multiprocessing mode
        return self._process_files_multiprocess(file_batches)
    
    def _process_files_single(self, files: List[FileInfo]) -> List[ProcessingResult]:
        """Process files in single process mode"""
        results = []
        
        if self.show_progress:
            console = Console()
            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                "({task.completed}/{task.total})",
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Processing files", total=len(files))
                
                for file_info in files:
                    try:
                        # Create progress callback for single-threaded mode
                        def progress_callback(bytes_processed, file_size):
                            progress_percent = (bytes_processed / file_size) * 100
                            progress.update(task, description=f"Processing {file_info.relative_path.name} ({progress_percent:.1f}%)")
                        
                        file_hash = self.algorithm.hash_file(file_info.path, progress_callback=progress_callback)
                        secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
                        other_hash = self.algorithm.hash_data(secondary_data)
                        
                        result = ProcessingResult(file_info, file_hash, other_hash)
                        results.append(result)
                        
                        progress.update(task, advance=1, description=f"✓ {file_info.relative_path.name}")
                        
                    except HashError as e:
                        result = ProcessingResult(file_info, error=str(e))
                        results.append(result)
                        progress.update(task, advance=1, description=f"✗ Error: {file_info.relative_path.name}")
        else:
            # No progress reporting
            for file_info in files:
                try:
                    file_hash = self.algorithm.hash_file(file_info.path)
                    secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
                    other_hash = self.algorithm.hash_data(secondary_data)
                    
                    result = ProcessingResult(file_info, file_hash, other_hash)
                    results.append(result)
                    
                except HashError as e:
                    result = ProcessingResult(file_info, error=str(e))
                    results.append(result)
        
        return results
    
    def _process_files_multiprocess(self, file_batches: List[List[FileInfo]]) -> List[ProcessingResult]:
        """Process files using multiple processes"""
        result_queue = mp.Queue()
        progress_queue = mp.Queue()
        processes = []
        
        # Start worker processes
        for thread_id, batch in enumerate(file_batches):
            if not batch:  # Skip empty batches
                continue
                
            from .hash_algorithms import get_algorithm_key
            process = Process(
                target=self._process_batch_worker,
                args=(batch, get_algorithm_key(self.algorithm), result_queue, progress_queue, thread_id)
            )
            process.start()
            processes.append(process)
        
        results = []
        
        if self.show_progress:
            # Collect all files for progress tracking
            total_files = sum(len(batch) for batch in file_batches)
            
            console = Console()
            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                "({task.completed}/{task.total})",
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                # Create individual progress bars for each batch/thread
                tasks = []
                for i, batch in enumerate(file_batches):
                    if batch:  # Only create task for non-empty batches
                        task_id = progress.add_task(
                            f"Thread {i+1}: Processing files", 
                            total=len(batch)
                        )
                        tasks.append((task_id, len(batch)))
                
                # Track completed files per thread
                completed_per_thread = [0] * len(tasks)
                current_files = [""] * len(tasks)  # Track current file being processed
                file_progress = [0.0] * len(tasks)  # Track file-level progress
                
                # Set up signal handler for CTRL-C
                def signal_handler(signum, frame):
                    self.should_stop = True
                    console.print("\n[yellow]Received interrupt signal. Stopping gracefully...[/yellow]")
                
                signal.signal(signal.SIGINT, signal_handler)
                
                # Collect results and update progress
                last_write_time = time.time()
                write_interval = 5.0  # Write hashes every 5 seconds
                
                while not self.should_stop:
                    # Check for progress updates
                    try:
                        while True:
                            update = progress_queue.get_nowait()
                            
                            # Get thread ID from the update
                            thread_id = update.get('thread_id', 0)
                            
                            if thread_id < len(tasks):
                                task_id, _ = tasks[thread_id]
                                filename = update['filename']
                                if len(filename) > 25:
                                    filename = filename[:22] + "..."
                                
                                update_type = update.get('type', 'file_complete')
                                
                                if update_type == 'file_start':
                                    current_files[thread_id] = filename
                                    file_progress[thread_id] = 0.0
                                    progress.update(
                                        task_id, 
                                        description=f"Thread {thread_id+1}: {filename}"
                                    )
                                elif update_type == 'file_progress':
                                    file_total = update.get('file_total', 1)
                                    file_progress_bytes = update.get('file_progress', 0)
                                    file_progress[thread_id] = (file_progress_bytes / file_total) * 100
                                    
                                    progress.update(
                                        task_id, 
                                        description=f"Thread {thread_id+1}: {filename} ({file_progress[thread_id]:.1f}%)"
                                    )
                                elif update_type == 'file_complete':
                                    completed_per_thread[thread_id] += 1
                                    status = "✓" if update.get('success', True) else "✗"
                                    current_files[thread_id] = ""
                                    
                                    progress.update(
                                        task_id, 
                                        advance=1, 
                                        description=f"Thread {thread_id+1}: {status} {filename}"
                                    )
                    except:
                        pass
                    
                    # Check for results
                    try:
                        result = result_queue.get(timeout=0.1)
                        results.append(result)
                    except:
                        pass
                    
                    # Check if all files are completed
                    total_completed = sum(completed_per_thread)
                    if total_completed >= total_files:
                        break
                    
                    # Periodic hash writing
                    current_time = time.time()
                    if current_time - last_write_time > write_interval:
                        # Trigger periodic write (this will be handled by the caller)
                        last_write_time = current_time
                    
                    # Small delay to prevent busy waiting
                    time.sleep(0.01)
                
                # Collect any remaining results
                while not result_queue.empty():
                    try:
                        result = result_queue.get_nowait()
                        results.append(result)
                    except:
                        break
                
                # Clean up processes if stopped early
                if self.should_stop:
                    console.print("[yellow]Stopping worker processes...[/yellow]")
                    for process in processes:
                        if process.is_alive():
                            process.terminate()
                            process.join(timeout=2)
                            if process.is_alive():
                                process.kill()
        else:
            # No progress reporting - just collect results
            for process in processes:
                process.join()
            
            while not result_queue.empty():
                try:
                    result = result_queue.get_nowait()
                    results.append(result)
                except:
                    break
        
        # Wait for all processes to complete
        for process in processes:
            if process.is_alive():
                process.join()
        
        return results
    
    def _process_batch_worker(self, files: List[FileInfo], algorithm_name: str, result_queue: Queue, progress_queue: Queue, thread_id: int = 0):
        """Worker function to process a batch of files"""
        for file_info in files:
            process_file_worker(file_info, algorithm_name, result_queue, progress_queue, thread_id)
    
    def get_summary_stats(self, results: List[ProcessingResult]) -> Dict[str, Any]:
        """Get summary statistics from processing results"""
        total_files = len(results)
        successful = sum(1 for r in results if r.success)
        failed = total_files - successful
        
        total_size = sum(r.file_info.size for r in results if r.success)
        
        return {
            'total_files': total_files,
            'successful': successful,
            'failed': failed,
            'total_size': total_size,
            'errors': [r.error for r in results if not r.success]
        }
    
    def print_summary(self, results: List[ProcessingResult]):
        """Print processing summary"""
        stats = self.get_summary_stats(results)
        
        console = Console()
        table = Table(title="Processing Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total files", str(stats['total_files']))
        table.add_row("Successful", str(stats['successful']))
        table.add_row("Failed", str(stats['failed']))
        table.add_row("Total size", f"{stats['total_size']:,} bytes")
        
        console.print(table)
        
        if stats['errors']:
            console.print("\n[bold red]Errors encountered:[/bold red]")
            for error in stats['errors']:
                console.print(f"  • {error}")
