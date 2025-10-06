"""
Simplified file hashing processor using only ProcessPoolExecutor
"""

import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import select
import threading

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
    errors: int = 0


@dataclass
class WorkerMessage:
    """Message sent from worker to main process via pipe"""
    message_type: str  # 'hash_result', 'progress', 'complete', 'error'
    worker_id: int
    data: Any
    timestamp: float


@dataclass
class ProgressUpdate:
    """Progress update from worker"""
    files_processed: int
    bytes_processed: int
    current_file: Optional[str] = None


def process_files_batch_with_pipe(files: List[FileInfo], algorithm: HashAlgorithm, 
                                 worker_id: int, pipe_conn) -> Dict[str, Any]:
    """
    Process a batch of files using a single worker process with pipe communication.
    This function sends results and progress updates via the pipe while still
    returning results for ProcessPoolExecutor compatibility.
    
    Args:
        files: List of FileInfo objects to process
        algorithm: Hash algorithm to use
        worker_id: Worker identifier for logging
        pipe_conn: Pipe connection to send messages to main process
    
    Returns:
        Dictionary with results and statistics (for ProcessPoolExecutor)
    """
    if not files:
        # Send completion message
        message = WorkerMessage(
            message_type='complete',
            worker_id=worker_id,
            data=WorkerStats(
                worker_id=worker_id,
                files_processed=0,
                bytes_processed=0,
                total_time=0.0,
                errors=0
            ),
            timestamp=time.time()
        )
        try:
            pipe_conn.send(message)
        except:
            pass  # Pipe might be closed
        
        return {
            'results': [],
            'stats': WorkerStats(
                worker_id=worker_id,
                files_processed=0,
                bytes_processed=0,
                total_time=0.0,
                errors=0
            )
        }
    
    results = []
    processor = HashProcessor(algorithm)
    start_time = time.time()
    files_processed = 0
    bytes_processed = 0
    errors = 0
    
    try:
        for file_info in files:
            try:
                file_start_time = time.time()
                primary_hash = processor.compute_file_hash(file_info.path)
                secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
                secondary_hash = processor.compute_data_hash(secondary_data)
                
                result = HashResult(
                    file_info=file_info,
                    primary_hash=primary_hash,
                    secondary_hash=secondary_hash,
                    processing_time=time.time() - file_start_time,
                    bytes_processed=file_info.size,
                    success=True
                )
                results.append(result)
                files_processed += 1
                bytes_processed += file_info.size
                
                # Send hash result immediately via pipe
                message = WorkerMessage(
                    message_type='hash_result',
                    worker_id=worker_id,
                    data=result,
                    timestamp=time.time()
                )
                try:
                    pipe_conn.send(message)
                except:
                    pass  # Pipe might be closed
                
                # Send progress update every 10 files or every 100MB
                if files_processed % 10 == 0 or bytes_processed % (100 * 1024 * 1024) == 0:
                    progress = ProgressUpdate(
                        files_processed=files_processed,
                        bytes_processed=bytes_processed,
                        current_file=str(file_info.path)
                    )
                    message = WorkerMessage(
                        message_type='progress',
                        worker_id=worker_id,
                        data=progress,
                        timestamp=time.time()
                    )
                    try:
                        pipe_conn.send(message)
                    except:
                        pass  # Pipe might be closed
                
            except Exception as e:
                result = HashResult(
                    file_info=file_info,
                    primary_hash="",
                    secondary_hash="",
                    processing_time=time.time() - file_start_time,
                    bytes_processed=0,
                    success=False,
                    error=str(e)
                )
                results.append(result)
                errors += 1
                logging.error(f"Worker {worker_id} error processing {file_info.path}: {e}")
                
                # Send error result via pipe
                message = WorkerMessage(
                    message_type='hash_result',
                    worker_id=worker_id,
                    data=result,
                    timestamp=time.time()
                )
                try:
                    pipe_conn.send(message)
                except:
                    pass  # Pipe might be closed
        
        # Send final completion message
        total_time = time.time() - start_time
        final_stats = WorkerStats(
            worker_id=worker_id,
            files_processed=files_processed,
            bytes_processed=bytes_processed,
            total_time=total_time,
            errors=errors
        )
        
        message = WorkerMessage(
            message_type='complete',
            worker_id=worker_id,
            data=final_stats,
            timestamp=time.time()
        )
        try:
            pipe_conn.send(message)
        except:
            pass  # Pipe might be closed
        
        return {
            'results': results,
            'stats': final_stats
        }
        
    except Exception as e:
        # Send error message
        error_message = WorkerMessage(
            message_type='error',
            worker_id=worker_id,
            data=str(e),
            timestamp=time.time()
        )
        try:
            pipe_conn.send(error_message)
        except:
            pass  # Pipe might be closed
        logging.error(f"Worker {worker_id} fatal error: {e}")
        
        # Return empty results on fatal error
        return {
            'results': [],
            'stats': WorkerStats(
                worker_id=worker_id,
                files_processed=0,
                bytes_processed=0,
                total_time=time.time() - start_time,
                errors=1
            )
        }
    
    finally:
        try:
            pipe_conn.close()
        except:
            pass


def process_files_batch(files: List[FileInfo], algorithm: HashAlgorithm, worker_id: int = 0) -> Dict[str, Any]:
    """
    Process a batch of files using a single worker process.
    This function is designed to be called by ProcessPoolExecutor.
    
    Args:
        files: List of FileInfo objects to process
        algorithm: Hash algorithm to use
        worker_id: Worker identifier for logging
    
    Returns:
        Dictionary with results and statistics
    """
    if not files:
        return {
            'results': [],
            'stats': WorkerStats(
                worker_id=worker_id,
                files_processed=0,
                bytes_processed=0,
                total_time=0.0,
                errors=0
            )
        }
    
    results = []
    processor = HashProcessor(algorithm)
    start_time = time.time()
    files_processed = 0
    bytes_processed = 0
    errors = 0
    
    for file_info in files:
        try:
            file_start_time = time.time()
            primary_hash = processor.compute_file_hash(file_info.path)
            secondary_data = f"{file_info.size}:{file_info.mtime}".encode()
            secondary_hash = processor.compute_data_hash(secondary_data)
            
            result = HashResult(
                file_info=file_info,
                primary_hash=primary_hash,
                secondary_hash=secondary_hash,
                processing_time=time.time() - file_start_time,
                bytes_processed=file_info.size,
                success=True
            )
            results.append(result)
            files_processed += 1
            bytes_processed += file_info.size
            
        except Exception as e:
            result = HashResult(
                file_info=file_info,
                primary_hash="",
                secondary_hash="",
                processing_time=time.time() - file_start_time,
                bytes_processed=0,
                success=False,
                error=str(e)
            )
            results.append(result)
            errors += 1
            logging.error(f"Worker {worker_id} error processing {file_info.path}: {e}")
    
    total_time = time.time() - start_time
    
    return {
        'results': results,
        'stats': WorkerStats(
            worker_id=worker_id,
            files_processed=files_processed,
            bytes_processed=bytes_processed,
            total_time=total_time,
            errors=errors
        )
    }


class FileHashProcessor:
    """Simplified hash processor using only ProcessPoolExecutor"""
    
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.MD5, 
                 num_workers: Optional[int] = None, quiet: bool = False):
        self.algorithm = algorithm
        self.num_workers = num_workers or mp.cpu_count()
        self.quiet = quiet
        self.console = Console()
        self.results: List[HashResult] = []
        self.total_processing_time: float = 0.0
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def process_files(self, file_lists: List[List[FileInfo]]) -> List[HashResult]:
        """
        Process files using ProcessPoolExecutor
        
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
        all_stats = []
        processed_count = 0
        processed_bytes = 0
        last_update = time.time()
        
        # Use ProcessPoolExecutor for processing
        with ProcessPoolExecutor(max_workers=len(file_lists)) as executor:
            # Submit all batches
            future_to_batch = {}
            for i, files in enumerate(file_lists):
                if files:  # Only submit non-empty batches
                    future = executor.submit(process_files_batch, files, self.algorithm, i)
                    future_to_batch[future] = (i, files)
            
            # Process completed batches as they finish
            for future in as_completed(future_to_batch):
                batch_id, batch_files = future_to_batch[future]
                try:
                    batch_data = future.result()
                    batch_results = batch_data['results']
                    batch_stats = batch_data['stats']
                    
                    all_results.extend(batch_results)
                    all_stats.append(batch_stats)
                    
                    # Update progress
                    batch_processed = len(batch_results)
                    batch_bytes = sum(r.bytes_processed for r in batch_results)
                    processed_count += batch_processed
                    processed_bytes += batch_bytes
                    
                    # Show progress updates - more frequent for large datasets
                    progress_interval = max(1000, total_files // 100)  # More frequent updates
                    if not self.quiet and (time.time() - last_update > 5.0 or processed_count % progress_interval == 0):
                        file_percentage = (processed_count / total_files) * 100
                        bytes_percentage = (processed_bytes / total_bytes) * 100
                        elapsed_time = time.time() - start_time
                        rate = processed_count / elapsed_time if elapsed_time > 0 else 0
                        eta_seconds = (total_files - processed_count) / rate if rate > 0 else 0
                        eta_minutes = eta_seconds / 60
                        
                        self.console.print(f"[blue]Processed {processed_count:,}/{total_files:,} files ({file_percentage:.1f}%), {processed_bytes:,}/{total_bytes:,} bytes ({bytes_percentage:.1f}%) - Rate: {rate:.1f} files/sec - ETA: {eta_minutes:.1f} min[/blue]")
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
        self.worker_stats = all_stats
        
        return all_results
    
    def process_files_with_pipes(self, file_lists: List[List[FileInfo]], 
                                output_file: Optional[Path] = None) -> Dict[str, Any]:
        """
        Process files using ProcessPoolExecutor with pipe communication for real-time updates
        
        Args:
            file_lists: List of file lists, one per worker
            output_file: Optional output file for streaming results
        
        Returns:
            Dictionary with processing statistics
        """
        if not file_lists:
            return {'total_files': 0, 'successful': 0, 'failed': 0, 'total_bytes': 0, 'total_time': 0.0}
        
        # Calculate totals for progress reporting
        total_files = sum(len(files) for files in file_lists)
        total_bytes = sum(sum(f.size for f in files) for files in file_lists)
        
        if not self.quiet:
            self.console.print(f"[blue]Processing {total_files:,} files using ProcessPoolExecutor with pipes...[/blue]")
        
        # Create pipes for each worker
        pipes = []
        child_connections = []  # Store child connections to close them later
        start_time = time.time()
        
        # Set up hash file if streaming to file
        hash_file = None
        if output_file:
            try:
                from .hash_file import HashFile
            except ImportError:
                from hash_file import HashFile
            hash_file = HashFile(output_file)
            base_directory = output_file.parent
            # Create header
            hash_file.write([], base_directory, self.algorithm, update_mode=False)
        
        # Track progress
        progress_data = {
            'processed_count': 0,
            'processed_bytes': 0,
            'successful': 0,
            'failed': 0,
            'total_files': total_files,
            'total_bytes': total_bytes,
            'start_time': start_time,
            'last_update': start_time
        }
        
        try:
            # Create pipes and submit jobs to ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=len(file_lists)) as executor:
                # Create pipes and submit all batches
                future_to_batch = {}
                for i, files in enumerate(file_lists):
                    if files:  # Only create pipes for non-empty batches
                        parent_conn, child_conn = mp.Pipe()
                        pipes.append(parent_conn)
                        child_connections.append(child_conn)
                        
                        future = executor.submit(process_files_batch_with_pipe, files, self.algorithm, i, child_conn)
                        future_to_batch[future] = (i, files)
                
                # Start pipe listener thread
                pipe_thread = threading.Thread(target=self._listen_to_pipes, 
                                             args=(pipes, hash_file, base_directory if output_file else None, progress_data))
                pipe_thread.daemon = True
                pipe_thread.start()
                
                # Wait for all futures to complete (ProcessPoolExecutor results)
                all_results = []
                all_stats = []
                for future in as_completed(future_to_batch):
                    batch_id, batch_files = future_to_batch[future]
                    try:
                        batch_data = future.result()
                        batch_results = batch_data['results']
                        batch_stats = batch_data['stats']
                        
                        all_results.extend(batch_results)
                        all_stats.append(batch_stats)
                        
                    except Exception as e:
                        logging.error(f"Batch {batch_id} failed: {e}")
                        if not self.quiet:
                            self.console.print(f"[red]Batch {batch_id} failed: {e}[/red]")
                
                # Wait a bit for pipe thread to finish processing remaining messages
                pipe_thread.join(timeout=1.0)
            
            # Store results
            self.total_processing_time = time.time() - start_time
            self.results = all_results
            self.worker_stats = all_stats
            
            if not self.quiet:
                self.console.print(f"[green]Completed processing {progress_data['processed_count']:,}/{total_files:,} files (100.0%), {progress_data['processed_bytes']:,}/{total_bytes:,} bytes (100.0%)[/green]")
            
            return {
                'total_files': total_files,
                'successful': progress_data['successful'],
                'failed': progress_data['failed'],
                'total_bytes': progress_data['processed_bytes'],
                'total_time': self.total_processing_time,
                'avg_speed': (progress_data['processed_bytes'] / (1024 * 1024)) / self.total_processing_time if self.total_processing_time > 0 else 0.0,
                'worker_stats': list(all_stats)
            }
            
        finally:
            # Clean up pipes
            for pipe in pipes:
                if not pipe.closed:
                    pipe.close()
            for child_conn in child_connections:
                if not child_conn.closed:
                    child_conn.close()
    
    def _listen_to_pipes(self, pipes: List[Any], hash_file: Optional[Any], 
                        base_directory: Optional[Path], progress_data: Dict[str, Any]) -> None:
        """Listen to pipes for real-time messages from workers"""
        completed_workers = 0
        worker_stats = {}
        
        while completed_workers < len(pipes):
            # Use select to check for ready pipes (Unix only)
            if hasattr(select, 'select'):
                ready_pipes = []
                for pipe in pipes:
                    if pipe.poll():
                        ready_pipes.append(pipe)
            else:
                # Windows fallback - check all pipes
                ready_pipes = [pipe for pipe in pipes if pipe.poll()]
            
            if not ready_pipes:
                time.sleep(0.01)  # Small delay to prevent busy waiting
                continue
            
            # Process messages from ready pipes
            for pipe in ready_pipes:
                try:
                    message = pipe.recv()
                    print("Mensaje recibido del pipe:", message)
                    
                    # Handle message based on type
                    if message.message_type == 'hash_result':
                        progress_data['processed_count'] += 1
                        if message.data.success:
                            progress_data['successful'] += 1
                            progress_data['processed_bytes'] += message.data.bytes_processed
                        else:
                            progress_data['failed'] += 1
                            
                        # Stream to file if enabled
                        if hash_file and message.data.success:
                            try:
                                from .hash_file import HashEntry
                            except ImportError:
                                from hash_file import HashEntry
                            entry = HashEntry(
                                primary_hash=message.data.primary_hash,
                                secondary_hash=message.data.secondary_hash,
                                directory=message.data.file_info.directory,
                                filename=message.data.file_info.filename,
                                size=message.data.file_info.size,
                                inode=message.data.file_info.inode,
                                mtime=message.data.file_info.mtime,
                                is_symlink=message.data.file_info.is_symlink
                            )
                            hash_file.write([entry], base_directory, self.algorithm, update_mode=True)
                    
                    elif message.message_type == 'complete':
                        completed_workers += 1
                        worker_stats[message.worker_id] = message.data
                    
                    # Handle progress and error messages
                    self._handle_worker_message(message, progress_data)
                        
                except EOFError:
                    # Pipe closed
                    completed_workers += 1
                except Exception as e:
                    self.logger.error(f"Error receiving message: {e}")
    
    def _handle_worker_message(self, message: WorkerMessage, progress_data: Dict[str, Any]) -> None:
        """Handle a message from a worker process"""
        
        if message.message_type == 'progress' and not self.quiet:
            # Show progress update
            current_time = time.time()
            if current_time - progress_data['last_update'] > 2.0:  # Update every 2 seconds
                file_percentage = (progress_data['processed_count'] / progress_data['total_files']) * 100 if progress_data['total_files'] > 0 else 0
                bytes_percentage = (progress_data['processed_bytes'] / progress_data['total_bytes']) * 100 if progress_data['total_bytes'] > 0 else 0
                elapsed_time = current_time - progress_data['start_time']
                rate = progress_data['processed_count'] / elapsed_time if elapsed_time > 0 else 0
                eta_seconds = (progress_data['total_files'] - progress_data['processed_count']) / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                self.console.print(f"[blue]Processed {progress_data['processed_count']:,}/{progress_data['total_files']:,} files ({file_percentage:.1f}%), {progress_data['processed_bytes']:,}/{progress_data['total_bytes']:,} bytes ({bytes_percentage:.1f}%) - Rate: {rate:.1f} files/sec - ETA: {eta_minutes:.1f} min[/blue]")
                progress_data['last_update'] = current_time
        
        elif message.message_type == 'error':
            self.logger.error(f"Worker {message.worker_id} error: {message.data}")
            if not self.quiet:
                self.console.print(f"[red]Worker {message.worker_id} error: {message.data}[/red]")
    
    def process_files_streaming(self, file_lists: List[List[FileInfo]], output_file: Path, 
                               algorithm: HashAlgorithm) -> Dict[str, Any]:
        """
        Process files using ProcessPoolExecutor with streaming writes to handle very large datasets
        
        Args:
            file_lists: List of file lists, one per worker
            output_file: Path to output hash file
            algorithm: Hash algorithm to use
        
        Returns:
            Dictionary with processing statistics
        """
        if not file_lists:
            return {'total_files': 0, 'successful': 0, 'failed': 0, 'total_bytes': 0, 'total_time': 0.0}
        
        # Track total processing time
        start_time = time.time()
        
        # Calculate total files and bytes for progress reporting
        total_files = sum(len(files) for files in file_lists)
        total_bytes = sum(sum(f.size for f in files) for files in file_lists)
        
        if not self.quiet:
            self.console.print(f"[blue]Processing {total_files:,} files using streaming ProcessPoolExecutor...[/blue]")
        
        processed_count = 0
        processed_bytes = 0
        successful = 0
        failed = 0
        last_update = time.time()
        
        # Write header first
        from .hash_file import HashFile
        hash_file = HashFile(output_file)
        base_directory = output_file.parent
        
        # Create header entries (empty list to just write header)
        hash_file.write([], base_directory, algorithm, update_mode=False)
        
        # Use ProcessPoolExecutor for processing with streaming writes
        with ProcessPoolExecutor(max_workers=len(file_lists)) as executor:
            # Submit all batches
            future_to_batch = {}
            for i, files in enumerate(file_lists):
                if files:  # Only submit non-empty batches
                    future = executor.submit(process_files_batch, files, algorithm, i)
                    future_to_batch[future] = (i, files)
            
            # Process completed batches and write immediately
            batch_count = 0
            for future in as_completed(future_to_batch):
                batch_id, batch_files = future_to_batch[future]
                try:
                    batch_data = future.result()
                    batch_results = batch_data['results']
                    batch_stats = batch_data['stats']
                    
                    # Convert results to hash entries and write immediately
                    entries = []
                    for result in batch_results:
                        if result.success:
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
                            entries.append(entry)
                            successful += 1
                        else:
                            failed += 1
                    
                    # Write batch to file (append mode)
                    if entries:
                        hash_file.write(entries, base_directory, algorithm, update_mode=True)
                    
                    # Update progress
                    batch_processed = len(batch_results)
                    batch_bytes = sum(r.bytes_processed for r in batch_results)
                    processed_count += batch_processed
                    processed_bytes += batch_bytes
                    batch_count += 1
                    
                    # Show progress updates - more frequent for large datasets
                    progress_interval = max(1000, total_files // 100)
                    if not self.quiet and (time.time() - last_update > 5.0 or processed_count % progress_interval == 0):
                        file_percentage = (processed_count / total_files) * 100
                        bytes_percentage = (processed_bytes / total_bytes) * 100
                        elapsed_time = time.time() - start_time
                        rate = processed_count / elapsed_time if elapsed_time > 0 else 0
                        eta_seconds = (total_files - processed_count) / rate if rate > 0 else 0
                        eta_minutes = eta_seconds / 60
                        
                        self.console.print(f"[blue]Processed {processed_count:,}/{total_files:,} files ({file_percentage:.1f}%), {processed_bytes:,}/{total_bytes:,} bytes ({bytes_percentage:.1f}%) - Rate: {rate:.1f} files/sec - ETA: {eta_minutes:.1f} min - Batches: {batch_count}[/blue]")
                        last_update = time.time()
                        
                except Exception as e:
                    logging.error(f"Batch {batch_id} failed: {e}")
                    if not self.quiet:
                        self.console.print(f"[red]Batch {batch_id} failed: {e}[/red]")
        
        # Store the total processing time
        self.total_processing_time = time.time() - start_time
        
        if not self.quiet:
            self.console.print(f"[green]Completed processing {processed_count:,}/{total_files:,} files (100.0%), {processed_bytes:,}/{total_bytes:,} bytes (100.0%)[/green]")
        
        return {
            'total_files': total_files,
            'successful': successful,
            'failed': failed,
            'total_bytes': processed_bytes,
            'total_time': self.total_processing_time,
            'avg_speed': (processed_bytes / (1024 * 1024)) / self.total_processing_time if self.total_processing_time > 0 else 0.0
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        if not self.results:
            return {
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'total_bytes': 0,
                'total_time': 0.0,
                'avg_speed': 0.0
            }
        
        total_files = len(self.results)
        successful = sum(1 for r in self.results if r.success)
        failed = total_files - successful
        total_bytes = sum(r.bytes_processed for r in self.results if r.success)
        total_time = self.total_processing_time
        avg_speed = (total_bytes / (1024 * 1024)) / total_time if total_time > 0 else 0.0
        
        return {
            'total_files': total_files,
            'successful': successful,
            'failed': failed,
            'total_bytes': total_bytes,
            'total_time': total_time,
            'avg_speed': avg_speed,
            'worker_stats': getattr(self, 'worker_stats', [])
        }
    
    def display_statistics(self):
        """Display processing statistics in a table"""
        stats = self.get_statistics()
        
        # Main statistics table
        main_table = Table(title="Processing Statistics")
        main_table.add_column("Metric", style="cyan")
        main_table.add_column("Value", style="magenta")
        
        main_table.add_row("Total Files", str(stats['total_files']))
        main_table.add_row("Successful", str(stats['successful']))
        main_table.add_row("Failed", str(stats['failed']))
        main_table.add_row("Total Bytes", f"{stats['total_bytes']:,}")
        main_table.add_row("Total Time", f"{stats['total_time']:.2f}s")
        main_table.add_row("Avg Speed", f"{stats['avg_speed']:.2f} MB/s")
        
        self.console.print(main_table)
        
        # Per-worker statistics table
        worker_stats = stats.get('worker_stats', [])
        if worker_stats and len(worker_stats) > 1:
            worker_table = Table(title="Per-Worker Statistics")
            worker_table.add_column("Worker", style="cyan")
            worker_table.add_column("Files", style="magenta")
            worker_table.add_column("Bytes", style="green")
            worker_table.add_column("Time", style="yellow")
            worker_table.add_column("Speed", style="blue")
            worker_table.add_column("Errors", style="red")
            
            for worker_stat in worker_stats:
                speed_mbps = (worker_stat.bytes_processed / (1024 * 1024)) / worker_stat.total_time if worker_stat.total_time > 0 else 0.0
                worker_table.add_row(
                    f"Worker {worker_stat.worker_id}",
                    str(worker_stat.files_processed),
                    f"{worker_stat.bytes_processed:,}",
                    f"{worker_stat.total_time:.2f}s",
                    f"{speed_mbps:.2f} MB/s",
                    str(worker_stat.errors)
                )
            
            self.console.print(worker_table)
