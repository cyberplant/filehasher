#!/usr/bin/env python3

import os
import hashlib
import sys
import time
import queue
import configparser
import signal
import atexit
import threading
from typing import Dict, List, Tuple, Optional, Any, Union
from pathlib import Path
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# Constants
DEFAULT_ALGORITHM = "md5"
CONFIG_FILE = os.path.expanduser("~/.filehasher")

# Supported algorithms
ALGORITHMS = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
    "blake2b": hashlib.blake2b,
    "blake2s": hashlib.blake2s,
}

def _asserted_open(filename: str, mode: str):
    """Open file with proper error handling."""
    try:
        return open(filename, mode)
    except OSError as e:
        print(f"Error opening {filename}: {e}")
        raise

def _create_hash_key(filepath: str, directory: Optional[str] = None) -> str:
    """Create a hash key for the file."""
    if directory:
        # Make path relative to the specified directory
        rel_path = os.path.relpath(filepath, directory)
    else:
        rel_path = os.path.relpath(filepath)
    
    # Normalize path separators
    return rel_path.replace(os.sep, "/")

def _calculate_hash(filepath: str, algorithm: str, verbose: bool = False) -> str:
    """Calculate hash of a file."""
    hash_func = ALGORITHMS[algorithm]
    hasher = hash_func()
    
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)
    except OSError as e:
        if verbose:
            print(f"Error reading {filepath}: {e}")
        raise
    
    return hasher.hexdigest()

def _collect_files(hash_file: str, collect_paths: bool = False, collect_sizes: bool = False, directory: Optional[str] = None, verbose: bool = False) -> Union[int, List[Tuple[str, int]]]:
    """Collect files to process. Returns count or list of (path, size) tuples."""
    files = []
    start_dir = directory if directory else "."
    
    for subdir, dirs, filenames in os.walk(start_dir):
        # Skip .git directories
        if '.git' in dirs:
            dirs.remove('.git')
        
        for filename in filenames:
            filepath = os.path.join(subdir, filename)
            
            # Skip the hash file itself
            if os.path.abspath(filepath) == os.path.abspath(hash_file):
                continue
            
            # Skip hidden files and directories
            if filename.startswith('.'):
                continue
            
            # Get file size
            try:
                file_size = os.path.getsize(filepath)
            except OSError as e:
                if verbose:
                    print(f"Skipping inaccessible file: {filepath} ({e})")
                continue
            
            if collect_paths:
                if collect_sizes:
                    files.append((filepath, file_size))
                else:
                    files.append(filepath)
            else:
                files.append(filepath)
    
    if collect_paths:
        return files
    else:
        return len(files)

def _distribute_files_by_size(files_with_sizes: List[Tuple[str, int]], workers: int, verbose: bool = False) -> List[List[str]]:
    """Distribute files among workers based on size for balanced workload."""
    if not files_with_sizes:
        return [[] for _ in range(workers)]
    
    # Sort files by size (largest first) for greedy distribution
    sorted_files = sorted(files_with_sizes, key=lambda x: x[1], reverse=True)
    
    # Initialize worker lists and total sizes
    worker_lists = [[] for _ in range(workers)]
    worker_sizes = [0] * workers
    
    # Greedy distribution: assign each file to the worker with smallest total size
    for filepath, file_size in sorted_files:
        # Find worker with smallest total size
        min_worker = min(range(workers), key=lambda i: worker_sizes[i])
        worker_lists[min_worker].append(filepath)
        worker_sizes[min_worker] += file_size
    
    # Print distribution info if verbose
    if verbose:
        total_size = sum(worker_sizes)
        print(f"Size-aware distribution: {len(files_with_sizes)} files, {total_size/1024/1024:.1f}MB total")
        for i, (files, size) in enumerate(zip(worker_lists, worker_sizes)):
            percentage = (size / total_size * 100) if total_size > 0 else 0
            print(f"  Worker {i+1}: {len(files)} files, {size/1024/1024:.1f}MB ({percentage:.1f}%)")
    
    return worker_lists

def _can_skip_file(hashkey: str, file_size: int, file_inode: int, file_mtime: float, cache: Dict, file_stat: Optional[Any] = None) -> bool:
    """Check if we can skip hashing this file."""
    if hashkey not in cache:
        return False
    
    cached_data = cache[hashkey]
    if len(cached_data) < 6:
        # Old format without timestamp, always re-process
        return False
    
    cached_size, cached_inode, cached_mtime = cached_data[3], cached_data[4], cached_data[5]
    
    # Don't skip if timestamps are invalid (before 1990 or 0)
    if cached_mtime == 0 or cached_mtime < 631152000:  # Jan 1, 1990
        return False
    
    return (cached_size == file_size and 
            cached_inode == file_inode and 
            cached_mtime == file_mtime)

def _process_worker_batch(worker_files: List[str], algorithm: str, update: bool, append: bool, verbose: bool, worker_id: int, progress_queue, cache: Dict, writer_queue=None) -> List[Tuple]:
    """Process a batch of files in a worker process."""
    results = []
    
    for filepath in worker_files:
        try:
            # Get file metadata
            file_stat = os.stat(filepath)
            file_size = file_stat.st_size
            file_inode = file_stat.st_ino
            file_mtime = file_stat.st_mtime
            
            # Create hash key
            hashkey = _create_hash_key(filepath)
            
            # Check if we can skip this file
            if _can_skip_file(hashkey, file_size, file_inode, file_mtime, cache, file_stat):
                if verbose:
                    progress_queue.put(('verbose', f"Skipped {os.path.basename(filepath)}"))
                continue
            
            # Send start processing message
            progress_queue.put(('start_processing', os.path.basename(filepath)))
            
            # Calculate hash
            hexdigest = _calculate_hash(filepath, algorithm, verbose)
            
            # Send processing message
            progress_queue.put(('processing', os.path.basename(filepath)))
            
            # Get relative paths
            subdir = os.path.dirname(filepath)
            filename = os.path.basename(filepath)
            
            # Send progress message
            progress_queue.put(('progress', os.path.basename(filepath)))
            
            # Send result directly to writer queue if available
            if writer_queue is not None:
                filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                subdir_encoded = (subdir.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                output = f"{hashkey}|{hexdigest}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                try:
                    writer_queue.put(output)
                except Exception as e:
                    if verbose:
                        progress_queue.put(('verbose', f"Error sending to writer queue: {e}"))
            
            # Return result for backward compatibility
            result = (hashkey, hexdigest, subdir, filename, file_size, file_inode, file_mtime, filepath)
            results.append(result)
            
        except Exception as e:
            if verbose:
                progress_queue.put(('verbose', f"Error processing {filepath}: {e}"))
            continue
    
    return results

class WriterThread:
    """Dedicated thread for writing hash results to file."""
    
    def __init__(self, hash_file: str, update: bool, append: bool, algorithm: str, write_frequency: int = 100):
        self.hash_file = hash_file
        self.update = update
        self.append = append
        self.algorithm = algorithm
        self.write_frequency = write_frequency
        self.outfile = None
        self.new_hash_file = None
        self.results_written = 0
        self._stop_event = threading.Event()
        self._thread = None
        self._cleanup_done = False
        self._writer_queue = None  # Will be set to multiprocessing.Queue
        
    def start(self, writer_queue=None):
        """Start the writer thread."""
        self.new_hash_file = self.hash_file + ".new"
        self._writer_queue = writer_queue
        
        if self.update:
            # Create a new file with algorithm header
            self.outfile = _asserted_open(self.new_hash_file, "w")
            self.outfile.write(f"# Algorithm: {self.algorithm}\n")
        else:
            if self.append:
                # Only appends new hashes
                self.outfile = _asserted_open(self.hash_file, "a")
            else:
                # Create a new file with algorithm header
                self.outfile = _asserted_open(self.hash_file, "w")
                self.outfile.write(f"# Algorithm: {self.algorithm}\n")
        
        # Register cleanup function
        atexit.register(self._cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start the writer thread
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()
        
        return self
    
    def stop(self):
        """Stop the writer thread."""
        self._stop_event.set()
        # Send sentinel value to stop the writer loop
        try:
            self._writer_queue.put(None)
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=2)
        self._cleanup()
    
    def send_result(self, result_data: str):
        """Send a result to the writer thread."""
        try:
            self._writer_queue.put(result_data)
        except Exception as e:
            print(f"⚠️  Error sending result to writer: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle interruption signals."""
        print(f"\n⚠️  Process interrupted (signal {signum}). Saving progress...")
        self._cleanup()
        # Force exit to prevent hanging
        os._exit(1)
    
    def _cleanup(self):
        """Clean up file handles and rename if needed."""
        if self._cleanup_done or not self.outfile:
            return
            
        self._cleanup_done = True
        try:
            self.outfile.flush()
            self.outfile.close()
            if self.update and self.new_hash_file and os.path.exists(self.new_hash_file):
                # Only rename if we have some content (more than just the header)
                if os.path.getsize(self.new_hash_file) > len(f"# Algorithm: {self.algorithm}\n"):
                    os.rename(self.new_hash_file, self.hash_file)
                    print(f"✅ Progress saved to {self.hash_file}")
                else:
                    os.remove(self.new_hash_file)
                    print("⚠️  No progress to save (only header written)")
            elif not self.update and not self.append:
                # For generate mode, check if we have content beyond the header
                if os.path.exists(self.hash_file) and os.path.getsize(self.hash_file) > len(f"# Algorithm: {self.algorithm}\n"):
                    print(f"✅ Progress saved to {self.hash_file}")
                else:
                    print("⚠️  No progress to save (only header written)")
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")
        finally:
            self.outfile = None
    
    def _writer_loop(self):
        """Main writer loop that processes results from the queue."""
        while not self._stop_event.is_set():
            try:
                # Get result from queue with timeout
                result_data = self._writer_queue.get(timeout=0.1)
                if result_data is None:  # Sentinel value to stop
                    break
                    
                # Write the result to file
                if not self.outfile or self._cleanup_done:
                    continue
                    
                try:
                    self.outfile.write(result_data + "\n")
                    self.results_written += 1
                    
                    # Flush periodically
                    if self.results_written % self.write_frequency == 0:
                        self.outfile.flush()
                except Exception as e:
                    print(f"⚠️  Error writing result: {e}")
                    
                self._writer_queue.task_done()
            except queue.Empty:
                # No results available, continue
                continue
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                # These are expected when the process is terminating - don't show errors
                continue
            except Exception as e:
                # Only show unexpected errors
                if not self._cleanup_done and str(e).strip():
                    print(f"⚠️  Error in writer loop: {e}")
                continue

def generate_hashes(hash_file: str, update: bool = False, append: bool = False,
                   algorithm: str = DEFAULT_ALGORITHM, show_progress: bool = True,
                   workers: Optional[int] = None, verbose: bool = False, 
                   directory: Optional[str] = None, write_frequency: int = 100) -> None:
    """Generate hash file for all files in specified directory tree.
    
    Args:
        hash_file: Path to the hash file to create/update
        update: Whether to update existing hash file
        append: Whether to append to existing hash file
        algorithm: Hash algorithm to use
        show_progress: Whether to show progress bars
        workers: Number of parallel workers (default: CPU count)
        verbose: Whether to show verbose output
        directory: Directory to process (default: current directory)
        write_frequency: Write to file every N entries (default: 100)
    """
    # Set up signal handling for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n⚠️  Process interrupted (signal {signum}). Shutting down...")
        os._exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Validate directory if provided
    if directory and not os.path.exists(directory):
        raise FileNotFoundError(f"Directory not found: {directory}")
    if directory and not os.path.isdir(directory):
        raise NotADirectoryError(f"Path is not a directory: {directory}")

    # Load existing hash file if updating
    cache = {}
    if update and os.path.exists(hash_file):
        cache, _ = _load_hashfile(hash_file, cache_data=cache)
        if verbose:
            print(f"Loaded {len(cache)} existing hashes from {hash_file}")
    elif append and os.path.exists(hash_file):
        cache, _ = _load_hashfile(hash_file, cache_data=cache)
        if verbose:
            print(f"Loaded {len(cache)} existing hashes from {hash_file}")

    # Count total files first for progress tracking (much faster than collecting all)
    total_files = _collect_files(hash_file, collect_paths=False, directory=directory)

    # Determine number of workers (always use parallel, default to 1 worker if not specified)
    if workers is None:
        workers = min(mp.cpu_count(), total_files) if total_files > 0 else 1
    else:
        workers = min(workers, total_files) if total_files > 0 else 1

    # Start the writer thread for parallel processing
    writer_thread = WriterThread(hash_file, update, append, algorithm, write_frequency)
    
    # Use Rich progress bars for parallel processing with individual worker progress
    if show_progress:
        import threading
        console = Console()

        # Create individual progress bars for each worker
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TextColumn("[dim]{task.fields[filename]}"),
            console=console,
            refresh_per_second=10,
        ) as progress:
            # Create progress tasks for each worker with correct totals
            worker_tasks = []
            # We'll update these totals after we know the actual distribution
            for i in range(workers):
                task = progress.add_task(f"Worker {i+1}", total=0, filename="")
                worker_tasks.append(task)

            # Create progress queues for real-time updates using Manager
            with mp.Manager() as manager:
                progress_queues = [manager.Queue() for _ in range(workers)]
                worker_completed = manager.list([False] * workers)
                
                # Create multiprocessing queue for direct worker-to-writer communication
                writer_queue = manager.Queue()
                
                # Start the writer thread with the multiprocessing queue
                writer_thread.start(writer_queue)

                # Process files on-the-fly with parallel workers using shared list
                with ProcessPoolExecutor(max_workers=workers) as executor:
                    # Collect all files with sizes for balanced distribution
                    all_files_with_sizes = _collect_files(hash_file, collect_paths=True, collect_sizes=True, directory=directory, verbose=verbose)

                    # Distribute files among workers based on size for balanced workload
                    worker_file_lists = _distribute_files_by_size(all_files_with_sizes, workers, verbose)

                    # Update progress bar totals with actual file counts per worker
                    for i, worker_files in enumerate(worker_file_lists):
                        progress.update(worker_tasks[i], total=len(worker_files))

                    # Submit all worker tasks at once
                    future_to_worker = {}
                    for worker_id, worker_files in enumerate(worker_file_lists):
                        if worker_files:  # Only create task if worker has files
                            future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, progress_queues[worker_id], cache, writer_queue)
                            future_to_worker[future] = worker_id

                    # Function to monitor progress queues
                    def monitor_progress():
                        try:
                            while not all(worker_completed):
                                for worker_id in range(workers):
                                    if worker_completed[worker_id]:
                                        continue
                                    try:
                                        message = progress_queues[worker_id].get_nowait()
                                        if message[0] == 'start_processing':
                                            # Worker started processing a file
                                            progress.update(worker_tasks[worker_id], description=f"Worker {worker_id+1}", filename=message[1])
                                        elif message[0] == 'processing':
                                            # Worker is processing a file
                                            progress.update(worker_tasks[worker_id], description=f"Worker {worker_id+1}", filename=message[1])
                                        elif message[0] == 'progress':
                                            # Worker completed a file
                                            progress.update(worker_tasks[worker_id], advance=1)
                                        elif message[0] == 'verbose':
                                            # Verbose message from worker - show in progress bar description
                                            progress.update(worker_tasks[worker_id], description=f"Worker {worker_id+1}: {message[1]}")
                                    except queue.Empty:
                                        continue
                                    except Exception as e:
                                        # Ignore errors during cleanup
                                        pass
                            
                            # Process any remaining messages after workers are marked as completed
                            for worker_id in range(workers):
                                try:
                                    while True:
                                        message = progress_queues[worker_id].get_nowait()
                                        if message[0] == 'progress':
                                            progress.update(worker_tasks[worker_id], advance=1)
                                except queue.Empty:
                                    break
                                except Exception:
                                    break
                        except Exception as e:
                            # Ignore errors during cleanup
                            pass

                    # Start progress monitoring thread
                    monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                    monitor_thread.start()

                    # Wait for all workers to complete (results are sent directly to writer queue)
                    for future in as_completed(future_to_worker):
                        worker_id = future_to_worker[future]
                        try:
                            # Just wait for worker to complete - results are sent directly to writer queue
                            future.result()
                            
                        except Exception as e:
                            print(f"Error in worker {worker_id}: {e}")
                    
                    # Mark all workers as completed after all futures are done
                    for worker_id in range(workers):
                        worker_completed[worker_id] = True

                    # Wait for progress monitoring to complete
                    monitor_thread.join(timeout=1.0)
                    
                    # Ensure all progress bars show 100% completion
                    for i, worker_files in enumerate(worker_file_lists):
                        if worker_files:
                            progress.update(worker_tasks[i], completed=len(worker_files))
    else:
        # No progress display - still use multiprocessing queue for direct communication
        with mp.Manager() as manager:
            # Create multiprocessing queue for direct worker-to-writer communication
            writer_queue = manager.Queue()
            
            # Start the writer thread with the multiprocessing queue
            writer_thread.start(writer_queue)
            
            # Process files with parallel workers
            with ProcessPoolExecutor(max_workers=workers) as executor:
                # Collect all files with sizes for balanced distribution
                all_files_with_sizes = _collect_files(hash_file, collect_paths=True, collect_sizes=True, directory=directory, verbose=verbose)
                
                # Distribute files among workers based on size for balanced workload
                worker_file_lists = _distribute_files_by_size(all_files_with_sizes, workers, verbose)
                
                # Submit all worker tasks at once
                future_to_worker = {}
                for worker_id, worker_files in enumerate(worker_file_lists):
                    if worker_files:  # Only create task if worker has files
                        # Create dummy progress queue for workers
                        dummy_queue = manager.Queue()
                        future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, dummy_queue, cache, writer_queue)
                        future_to_worker[future] = worker_id
                
                # Wait for all workers to complete
                for future in as_completed(future_to_worker):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error in worker: {e}")

    # Stop the writer thread
    writer_thread.stop()

def _load_hashfile(filename: str, destDict: Optional[Dict] = None,
                   cache_data: Optional[Dict] = None) -> Tuple[Dict, Optional[str]]:
    """Load hash file and populate destination dictionary. Returns (dict, algorithm)."""
    if destDict is None:
        destDict = {}

    algorithm = None

    with open(filename, 'r') as f:
        for line in f:
            line = line.rstrip()
            if not line:
                continue

            # Check for algorithm header
            if line.startswith("# Algorithm: "):
                algorithm = line[13:].strip()
                continue

            # Parse hash entry
            parts = line.split("|")
            if len(parts) >= 5:
                hashkey = parts[0]
                hexdigest = parts[1]
                subdir = parts[2]
                filename = parts[3]
                file_size = int(parts[4])
                file_inode = int(parts[5]) if len(parts) > 5 else 0
                file_mtime = float(parts[6]) if len(parts) > 6 else 0
                
                destDict[hashkey] = (hexdigest, subdir, filename, file_size, file_inode, file_mtime)

    return destDict, algorithm

def _get_hash(s: str, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Get hash of a string."""
    hash_func = ALGORITHMS[algorithm]
    return hash_func(s.encode()).hexdigest()

def _getMD5(s):
    """Get MD5 hash of a string (legacy function)."""
    return _get_hash(s, "md5")

def _sorted_filenames(filelist: Dict) -> List[str]:
    """Get sorted list of filenames from filelist dictionary."""
    return sorted(filelist.keys())

def compare(hashfile1: str, hashfile2: str) -> None:
    """Compare two hash files and show differences."""
    print(f"Comparing {hashfile1} and {hashfile2}...")
    
    # Load both hash files
    dict1, _ = _load_hashfile(hashfile1)
    dict2, _ = _load_hashfile(hashfile2)
    
    # Find differences
    only_in_1 = set(dict1.keys()) - set(dict2.keys())
    only_in_2 = set(dict2.keys()) - set(dict1.keys())
    common = set(dict1.keys()) & set(dict2.keys())
    
    # Check for hash differences in common files
    different_hashes = []
    for filename in common:
        if dict1[filename][0] != dict2[filename][0]:  # Compare hash values
            different_hashes.append(filename)
    
    # Print results
    print(f"\nFiles only in {hashfile1}: {len(only_in_1)}")
    for filename in sorted(only_in_1):
        print(f"  {filename}")
    
    print(f"\nFiles only in {hashfile2}: {len(only_in_2)}")
    for filename in sorted(only_in_2):
        print(f"  {filename}")
    
    print(f"\nFiles with different hashes: {len(different_hashes)}")
    for filename in sorted(different_hashes):
        print(f"  {filename}")
    
    print(f"\nFiles with identical hashes: {len(common) - len(different_hashes)}")
    print(f"Total files in {hashfile1}: {len(dict1)}")
    print(f"Total files in {hashfile2}: {len(dict2)}")

def benchmark_algorithms(test_file: Optional[str] = None, algorithms: Optional[List[str]] = None, iterations: int = 3) -> Dict:
    """Benchmark hash algorithms."""
    if algorithms is None:
        algorithms = list(ALGORITHMS.keys())
    
    if test_file is None:
        # Create test data
        test_data = b"x" * (10 * 1024 * 1024)  # 10MB of data
    else:
        with open(test_file, 'rb') as f:
            test_data = f.read()
    
    results = {}
    
    for algorithm in algorithms:
        if algorithm not in ALGORITHMS:
            continue
            
        times = []
        for _ in range(iterations):
            start_time = time.time()
            hash_func = ALGORITHMS[algorithm]
            hasher = hash_func()
            hasher.update(test_data)
            hasher.hexdigest()
            end_time = time.time()
            times.append(end_time - start_time)
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        throughput = len(test_data) / avg_time / 1024 / 1024  # MB/s
        
        results[algorithm] = {
            'average_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'throughput_mb_per_sec': throughput
        }
    
    return results

def load_config() -> Dict:
    """Load configuration from file."""
    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)
        return dict(config['DEFAULT'])
    return {}

# Export supported algorithms
SUPPORTED_ALGORITHMS = ALGORITHMS

