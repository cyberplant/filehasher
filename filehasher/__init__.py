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
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
    from rich.console import Console
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

BLOCKSIZE = 1024 * 1024
SCRIPT_FILENAME = "filehasher_script.sh"
repeated: Dict[str, List] = {}

# Supported hash algorithms
SUPPORTED_ALGORITHMS = {
    'md5': hashlib.md5,
    'sha1': hashlib.sha1,
    'sha256': hashlib.sha256,
    'sha512': hashlib.sha512,
    'blake2b': hashlib.blake2b,
    'blake2s': hashlib.blake2s,
}

DEFAULT_ALGORITHM = 'md5'
CONFIG_FILE = '.filehasher.ini'


def _process_file_worker(file_info: Tuple[str, str, str, str, str, bool, bool]) -> Tuple[str, str, str, str, str, str, str, Optional[str]]:
    """
    Worker function for parallel file processing.
    Returns (hashkey, hexdigest, subdir, filename, file_size, file_inode, file_mtime, processed_filename)
    """
    subdir, filename, full_filename, algorithm, update, append, verbose = file_info

    if os.path.islink(full_filename):
        return None  # Skip symlinks

    file_stat = os.stat(full_filename)
    file_size = file_stat.st_size
    file_inode = file_stat.st_ino
    file_mtime = file_stat.st_mtime

    key = f"{full_filename}{file_size}{file_mtime}"
    hashkey = _get_hash(key, algorithm)

    processed_filename = full_filename if verbose else None

    try:
        with open(full_filename, "rb") as f:
            hashsum, _ = calculate_hash(f, algorithm, show_progress=False)
        hexdigest = hashsum.hexdigest()
    except Exception as e:
        print(f"Error processing {full_filename}: {e}")
        return None

    return hashkey, hexdigest, subdir, filename, str(file_size), str(file_inode), str(file_mtime), processed_filename


def _collect_files(hash_file: str, collect_paths: bool = False, collect_sizes: bool = False, directory: Optional[str] = None) -> Union[int, List[Tuple[str, str, str]], List[Tuple[str, str, str, int]]]:
    """
    Collect files from specified directory for processing.

    Args:
        hash_file: Name of the hash file to exclude
        collect_paths: If True, return list of (subdir, filename, full_filename)
        collect_sizes: If True, return list of (subdir, filename, full_filename, file_size)
                      If False, return count of files only
        directory: Directory to process (default: current directory)

    Returns:
        If collect_paths is False: total file count (int)
        If collect_paths is True and collect_sizes is False: list of file tuples
        If collect_paths is True and collect_sizes is True: list of file tuples with sizes
    """
    result = [] if collect_paths else 0
    start_dir = directory if directory else "."

    for subdir, dirs, files in os.walk(start_dir):
        if subdir == ".uma":
            continue
        if ".uma" in dirs:
            dirs.remove(".uma")

        for filename in files:
            if subdir == "." and (filename == hash_file or filename == hash_file + ".new"):
                continue
            full_filename = os.path.join(subdir, filename)
            if not os.path.islink(full_filename):
                if collect_paths:
                    if collect_sizes:
                        try:
                            file_size = os.path.getsize(full_filename)
                            result.append((subdir, filename, full_filename, file_size))
                        except OSError:
                            # Skip files we can't stat
                            continue
                    else:
                        result.append((subdir, filename, full_filename))
                else:
                    result += 1

    return result


def _process_worker_batch(worker_files: List[Tuple[str, str, str]], algorithm: str, update: bool, append: bool, verbose: bool, worker_id: int, progress_queue: Optional['mp.Queue'] = None, cache: Optional[dict] = None) -> List[Tuple]:
    """
    Process a batch of files for a single worker.
    This function runs in a separate process.
    """
    results = []
    skipped_count = 0  # Track skipped files for batched progress updates

    for subdir, filename, full_filename in worker_files:
        if os.path.islink(full_filename):
            # Send progress update even for skipped files
            if progress_queue:
                progress_queue.put(('progress', worker_id, 1, filename if verbose else None))
            continue

        # Send "start processing" message BEFORE starting to process the file
        if progress_queue and verbose:
            progress_queue.put(('start_processing', worker_id, 0, filename))

        file_stat = os.stat(full_filename)
        file_size = file_stat.st_size
        file_inode = file_stat.st_ino
        file_mtime = file_stat.st_mtime

        key = f"{full_filename}{file_size}{file_mtime}"
        hashkey = _get_hash(key, algorithm)

        processed_filename = full_filename if verbose else None

        # Check if we can skip this file (for update mode)
        if update and cache and hashkey in cache:
            cache_data = cache[hashkey]
            if _can_skip_file(full_filename, cache_data, verbose, progress_queue, worker_id, file_stat):
                # File is unchanged, batch progress updates
                skipped_count += 1
                # Return the cached data instead of recalculating
                results.append((hashkey, cache_data[0], subdir, filename, str(file_size), str(file_inode), str(file_mtime), processed_filename))
                continue
        
        try:
            with open(full_filename, "rb") as f:
                # Use a custom hash function that sends progress updates during processing
                hashsum, _ = calculate_hash_with_progress(f, algorithm, progress_queue, worker_id, filename, verbose)
            hexdigest = hashsum.hexdigest()

            results.append((hashkey, hexdigest, subdir, filename, str(file_size), str(file_inode), str(file_mtime), processed_filename))
        except Exception as e:
            print(f"Error processing {full_filename}: {e}")
            continue

        # Send progress update for each completed file
        if progress_queue:
            progress_queue.put(('progress', worker_id, 1, filename if verbose else None))

    # Send batched progress update for skipped files
    if progress_queue and skipped_count > 0:
        progress_queue.put(('progress', worker_id, skipped_count, f"Skipped {skipped_count} files" if verbose else None))

    # Signal completion
    if progress_queue:
        progress_queue.put(('done', worker_id))

    return results


def _get_hash(s: str, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Generate hash for a string using specified algorithm."""
    hash_func = SUPPORTED_ALGORITHMS.get(algorithm, hashlib.md5)
    return hash_func(s.encode("utf-8", "backslashreplace")).hexdigest()


def _distribute_files_by_size(files_with_sizes: List[Tuple[str, str, str, int]], workers: int, verbose: bool = False) -> List[List[Tuple[str, str, str]]]:
    """
    Distribute files among workers based on file sizes to balance workload.
    
    Uses a greedy algorithm to assign files to workers, always assigning
    the next file to the worker with the smallest current total size.
    
    Args:
        files_with_sizes: List of (subdir, filename, full_filename, file_size) tuples
        workers: Number of workers
        verbose: Whether to print distribution statistics
        
    Returns:
        List of worker file lists, where each worker gets (subdir, filename, full_filename) tuples
    """
    if not files_with_sizes or workers <= 0:
        return [[] for _ in range(workers)]
    
    # Sort files by size (largest first) for better distribution
    sorted_files = sorted(files_with_sizes, key=lambda x: x[3], reverse=True)
    
    # Initialize worker lists and their total sizes
    worker_lists = [[] for _ in range(workers)]
    worker_sizes = [0] * workers
    
    # Assign each file to the worker with the smallest current total size
    for subdir, filename, full_filename, file_size in sorted_files:
        # Find worker with smallest total size
        min_worker = min(range(workers), key=lambda i: worker_sizes[i])
        
        # Assign file to this worker
        worker_lists[min_worker].append((subdir, filename, full_filename))
        worker_sizes[min_worker] += file_size
    
    # Print distribution statistics if verbose
    if verbose:
        total_size = sum(worker_sizes)
        print(f"Size-aware distribution: {len(files_with_sizes)} files, {total_size / (1024*1024):.1f}MB total")
        for i, size in enumerate(worker_sizes):
            size_mb = size / (1024*1024)
            percentage = (size / total_size * 100) if total_size > 0 else 0
            print(f"  Worker {i+1}: {len(worker_lists[i])} files, {size_mb:.1f}MB ({percentage:.1f}%)")
    
    return worker_lists


def _can_skip_file(full_filename: str, cache_data: tuple, verbose: bool = False, progress_queue: Optional['mp.Queue'] = None, worker_id: int = 0, file_stat: Optional[os.stat_result] = None) -> bool:
    """
    Check if a file can be skipped based on size and modification time.
    
    Args:
        full_filename: Full path to the file
        cache_data: Tuple containing (hashsum, dirname, filename, file_size, file_inode, file_mtime)
        verbose: Whether to send skip messages via progress queue
        progress_queue: Queue to send verbose messages to (for parallel processing)
        worker_id: Worker ID for progress messages
        file_stat: Optional pre-retrieved file stats to avoid duplicate os.stat() calls
        
    Returns:
        True if file can be skipped, False otherwise
    """
    try:
        # Use provided file_stat or get it if not provided (for backward compatibility)
        if file_stat is None:
            file_stat = os.stat(full_filename)
        
        current_size = file_stat.st_size
        current_mtime = file_stat.st_mtime
        
        # Handle both old format (5 fields) and new format (6 fields)
        if len(cache_data) == 5:
            cached_size, cached_mtime = int(cache_data[3]), 0  # Old format has no timestamp
        else:
            cached_size, cached_mtime = int(cache_data[3]), float(cache_data[5])
        
        # Don't skip if the cached timestamp is 0 (old format) or very old (likely from archives)
        # Files with timestamps before 1990 are likely from archives and shouldn't be skipped
        if cached_mtime == 0 or cached_mtime < 631152000:  # 631152000 = Jan 1, 1990
            return False
        
        # Skip if size and modification time match
        if current_size == cached_size and current_mtime == cached_mtime:
            return True
            
    except (OSError, ValueError, IndexError):
        # If we can't stat the file or parse the cache data, don't skip
        pass
    
    return False


def calculate_hash(f, algorithm: str = DEFAULT_ALGORITHM, show_progress: bool = True) -> Tuple[Any, bool]:
    """Calculate hash for a file using specified algorithm."""
    hash_func = SUPPORTED_ALGORITHMS.get(algorithm, hashlib.md5)
    hashsum = hash_func()
    readcount = 0
    dirty = False

    while True:
        block = f.read(BLOCKSIZE)
        if not block:
            break
        readcount += 1
        if show_progress and readcount > 10 and readcount % 23 == 0:
            if dirty:
                sys.stdout.write("\b")
            dirty = True
            sys.stdout.write(("/", "|", "\\", "-")[readcount % 4])
            sys.stdout.flush()
        hashsum.update(block)

    return hashsum, dirty


def calculate_hash_with_progress(f, algorithm: str = DEFAULT_ALGORITHM, progress_queue: Optional['mp.Queue'] = None, worker_id: int = 0, filename: str = "", verbose: bool = False) -> Tuple[Any, bool]:
    """Calculate hash for a file with real-time progress updates via queue."""
    hash_func = SUPPORTED_ALGORITHMS.get(algorithm, hashlib.md5)
    hashsum = hash_func()
    readcount = 0
    dirty = False
    bytes_read = 0

    while True:
        block = f.read(BLOCKSIZE)
        if not block:
            break
        readcount += 1
        bytes_read += len(block)
        
        # Send progress updates during processing for large files (less frequently)
        if progress_queue and verbose and readcount > 0 and readcount % 100 == 0:
            # Send periodic updates during file processing (reduced frequency)
            progress_queue.put(('processing', worker_id, 0, filename, bytes_read))
        
        hashsum.update(block)

    return hashsum, dirty


# Backward compatibility
def _getMD5(s):
    return _get_hash(s, 'md5')


def calculate_md5(f):
    return calculate_hash(f, 'md5')


def benchmark_algorithms(test_file: str = None, algorithms: List[str] = None,
                        iterations: int = 3) -> Dict[str, Dict[str, float]]:
    """
    Benchmark different hash algorithms on a test file or sample data.

    Returns a dictionary with algorithm names as keys and performance metrics as values.
    """
    if algorithms is None:
        algorithms = list(SUPPORTED_ALGORITHMS.keys())

    if test_file and os.path.exists(test_file):
        print(f"Benchmarking algorithms using file: {test_file}")
        with open(test_file, "rb") as f:
            test_data = f.read()
    else:
        # Use sample data if no test file provided
        test_data = b"x" * (10 * 1024 * 1024)  # 10MB of data
        print("Benchmarking algorithms using 10MB sample data")

    results = {}

    for algorithm in algorithms:
        if algorithm not in SUPPORTED_ALGORITHMS:
            print(f"Warning: Unsupported algorithm '{algorithm}', skipping")
            continue

        times = []
        print(f"Testing {algorithm}...")

        for i in range(iterations):
            start_time = time.time()
            hash_func = SUPPORTED_ALGORITHMS[algorithm]
            hasher = hash_func()

            # Process data in chunks to simulate real file processing
            for i in range(0, len(test_data), BLOCKSIZE):
                chunk = test_data[i:i + BLOCKSIZE]
                hasher.update(chunk)

            end_time = time.time()
            times.append(end_time - start_time)

        avg_time = sum(times) / len(times)
        results[algorithm] = {
            'average_time': avg_time,
            'min_time': min(times),
            'max_time': max(times),
            'throughput_mb_per_sec': (len(test_data) / (1024 * 1024)) / avg_time
        }

    return results


def load_config() -> Dict[str, Any]:
    """
    Load configuration from .filehasher.ini file.

    Returns a dictionary with configuration values.
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if not config.has_section('filehasher'):
        config.add_section('filehasher')

    # Set defaults if not present
    if not config.has_option('filehasher', 'default_algorithm'):
        config.set('filehasher', 'default_algorithm', DEFAULT_ALGORITHM)

    if not config.has_option('filehasher', 'benchmark_iterations'):
        config.set('filehasher', 'benchmark_iterations', '3')

    if not config.has_option('filehasher', 'quiet'):
        config.set('filehasher', 'quiet', 'false')

    return {
        'default_algorithm': config.get('filehasher', 'default_algorithm'),
        'benchmark_iterations': config.getint('filehasher', 'benchmark_iterations'),
        'quiet': config.getboolean('filehasher', 'quiet'),
    }


def save_config(config_dict: Dict[str, Any]) -> None:
    """
    Save configuration to .filehasher.ini file.
    """
    config = configparser.ConfigParser()
    config.add_section('filehasher')

    for key, value in config_dict.items():
        config.set('filehasher', str(key), str(value))

    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)


def generate_hashes(hash_file: str, update: bool = False, append: bool = False,
                   algorithm: str = DEFAULT_ALGORITHM, show_progress: bool = True,
                   parallel: bool = False, workers: Optional[int] = None,
                   verbose: bool = False, directory: Optional[str] = None,
                   write_frequency: int = 100) -> None:
    """Generate hash file for all files in specified directory tree.
    
    Args:
        hash_file: Path to the hash file to create/update
        update: Whether to update existing hash file
        append: Whether to append to existing hash file
        algorithm: Hash algorithm to use
        show_progress: Whether to show progress bars
        parallel: Whether to use parallel processing
        workers: Number of parallel workers
        verbose: Whether to show verbose output
        directory: Directory to process (default: current directory)
        write_frequency: Write to file every N entries (default: 100)
    """
    # Validate directory if provided
    if directory and not os.path.exists(directory):
        raise FileNotFoundError(f"Directory not found: {directory}")
    if directory and not os.path.isdir(directory):
        raise NotADirectoryError(f"Path is not a directory: {directory}")
    
    cache = {}
    existing_algorithm = None

    if (append or update) and os.path.exists(hash_file):
        _, existing_algorithm = _load_hashfile(hash_file, cache_data=cache)

        # Check for algorithm mismatch
        if existing_algorithm and existing_algorithm != algorithm:
            print(f"⚠️  WARNING: Algorithm mismatch detected!")
            print(f"   Existing hash file uses: {existing_algorithm}")
            print(f"   Requested algorithm: {algorithm}")
            print(f"   This may cause inconsistent hash comparisons.")
            response = input("   Continue anyway? (y/N): ").strip().lower()
            if response not in ('y', 'yes'):
                print("   Operation cancelled.")
                return

    # Count total files first for progress tracking (much faster than collecting all)
    total_files = _collect_files(hash_file, collect_paths=False, directory=directory)

    # Determine number of workers
    if parallel:
        if workers is None:
            workers = min(mp.cpu_count(), total_files) if total_files > 0 else 1
        else:
            workers = min(workers, total_files) if total_files > 0 else 1
    else:
        workers = 1

    # Use WriterThread for parallel processing, HashFileWriter for sequential
    if parallel:
        # Start the writer thread for parallel processing
        writer_thread = WriterThread(hash_file, update, append, algorithm, write_frequency).start()
        
        # Use Rich progress bars for parallel processing with individual worker progress
        if show_progress and HAS_RICH:
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

                    # Process files on-the-fly with parallel workers using shared list
                    with ProcessPoolExecutor(max_workers=workers) as executor:
                        # Collect all files with sizes for balanced distribution
                        all_files_with_sizes = _collect_files(hash_file, collect_paths=True, collect_sizes=True, directory=directory)

                        # Distribute files among workers based on size for balanced workload
                        worker_file_lists = _distribute_files_by_size(all_files_with_sizes, workers, verbose)

                        # Update progress bar totals with actual file counts per worker
                        for i, worker_files in enumerate(worker_file_lists):
                            progress.update(worker_tasks[i], total=len(worker_files))

                        # Submit all worker tasks at once
                        future_to_worker = {}
                        for worker_id, worker_files in enumerate(worker_file_lists):
                            if worker_files:  # Only create task if worker has files
                                future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, progress_queues[worker_id], cache)
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
                                            if message[0] == 'progress':
                                                _, wid, advance, filename = message
                                                progress.update(worker_tasks[wid], advance=advance,
                                                              filename=os.path.basename(filename) if verbose and filename else "")
                                            elif message[0] == 'start_processing':
                                                _, wid, advance, filename = message
                                                # Update the display to show the file that's about to be processed
                                                progress.update(worker_tasks[wid], advance=advance,
                                                              filename=f"Starting: {os.path.basename(filename)}" if verbose and filename else "")
                                            elif message[0] == 'processing':
                                                _, wid, advance, filename, bytes_read = message
                                                # Show the file currently being processed with bytes read
                                                if verbose and filename:
                                                    size_mb = bytes_read / (1024 * 1024)
                                                    progress.update(worker_tasks[wid], advance=advance,
                                                                  filename=f"Processing: {os.path.basename(filename)} ({size_mb:.1f}MB)")
                                            elif message[0] == 'verbose':
                                                _, wid, advance, verbose_msg = message
                                                # Show verbose messages in the progress bar
                                                if verbose:
                                                    progress.update(worker_tasks[wid], advance=advance,
                                                                  filename=verbose_msg)
                                            elif message[0] == 'done':
                                                worker_completed[message[1]] = True
                                        except queue.Empty:
                                            # Queue is empty, continue monitoring
                                            pass
                                        except Exception as e:
                                            # Handle any other exceptions gracefully
                                            pass
                                    time.sleep(0)  # Yield control to avoid busy waiting
                            except Exception as e:
                                # Handle any exceptions in the monitoring loop gracefully
                                pass

                        # Start progress monitoring thread
                        monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                        monitor_thread.start()

                        # Process results as they complete - send directly to writer thread
                    
                        for future in as_completed(future_to_worker):
                            worker_id = future_to_worker[future]
                            try:
                                batch_results = future.result()
                                for result in batch_results:
                                    if result:
                                        hashkey, hexdigest, subdir, filename, file_size, file_inode, file_mtime, processed_filename = result
                                        filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                                        subdir_encoded = (subdir.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                                        if hashkey in cache:
                                            if update:
                                                cache_data = cache.pop(hashkey)
                                                # Handle both old format (5 fields) and new format (6 fields)
                                                if len(cache_data) == 5:
                                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                                                else:
                                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                                                results_buffer.append(output)
                                            else:
                                                # Not update mode, just write cached entry
                                                if len(cache_data) == 5:
                                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                                                else:
                                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                                                writer_thread.send_result(output)
                                        else:
                                            output = f"{hashkey}|{hexdigest}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                            writer_thread.send_result(output)
                            except Exception as e:
                                print(f"Error in worker {worker_id}: {e}")
                                # Mark worker as completed to avoid hanging
                                try:
                                    worker_completed[worker_id] = True
                                except Exception:
                                    # Ignore errors when trying to update shared objects during cleanup
                                    pass

                    # Wait for progress monitoring to complete
                    monitor_thread.join()

        elif parallel and show_progress and HAS_TQDM:
            # Fallback to tqdm for parallel processing with progress queues
            import threading
            progress_bar = tqdm(total=total_files, desc="Processing files", unit="file")

            # Collect all files with sizes for balanced distribution
            all_files_with_sizes = _collect_files(hash_file, collect_paths=True, collect_sizes=True, directory=directory)

            # Distribute files among workers based on size for balanced workload
            worker_file_lists = _distribute_files_by_size(all_files_with_sizes, workers, verbose)

            # Create progress queues for real-time updates using Manager
            with mp.Manager() as manager:
                progress_queues = [manager.Queue() for _ in range(workers)]
                worker_completed = manager.list([False] * workers)

                with ProcessPoolExecutor(max_workers=workers) as executor:
                    # Submit worker tasks
                    future_to_worker = {}
                    for worker_id, worker_files in enumerate(worker_file_lists):
                        if worker_files:
                            future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, progress_queues[worker_id], cache)
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
                                        if message[0] == 'progress':
                                            _, wid, advance, filename = message
                                            if verbose and filename:
                                                progress_bar.set_description(f"Worker {wid+1}: Completed {os.path.basename(filename)}")
                                            progress_bar.update(advance)
                                        elif message[0] == 'start_processing':
                                            _, wid, advance, filename = message
                                            if verbose and filename:
                                                progress_bar.set_description(f"Worker {wid+1}: Starting {os.path.basename(filename)}")
                                        elif message[0] == 'processing':
                                            _, wid, advance, filename, bytes_read = message
                                            if verbose and filename:
                                                size_mb = bytes_read / (1024 * 1024)
                                                progress_bar.set_description(f"Worker {wid+1}: Processing {os.path.basename(filename)} ({size_mb:.1f}MB)")
                                        elif message[0] == 'verbose':
                                            _, wid, advance, verbose_msg = message
                                            if verbose:
                                                progress_bar.set_description(f"Worker {wid+1}: {verbose_msg}")
                                        elif message[0] == 'done':
                                            worker_completed[message[1]] = True
                                    except queue.Empty:
                                        # Queue is empty, continue monitoring
                                        pass
                                    except Exception as e:
                                        # Handle any other exceptions gracefully
                                        pass
                                time.sleep(0)  # Yield control to avoid busy waiting
                        except Exception as e:
                            # Handle any exceptions in the monitoring loop gracefully
                            pass

                    # Start progress monitoring thread
                    monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                    monitor_thread.start()

                    # Process results - send directly to writer thread
                
                    for future in as_completed(future_to_worker):
                        worker_id = future_to_worker[future]
                        try:
                            batch_results = future.result()
                            for result in batch_results:
                                if result:
                                    hashkey, hexdigest, subdir, filename, file_size, file_inode, file_mtime, processed_filename = result
                                    filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                                    subdir_encoded = (subdir.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                                    if hashkey in cache:
                                        if update:
                                            cache_data = cache.pop(hashkey)
                                            # Handle both old format (5 fields) and new format (6 fields)
                                            if len(cache_data) == 5:
                                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                                            else:
                                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                                            results_buffer.append(output)
                                        else:
                                            # Not update mode, just write cached entry
                                            if len(cache_data) == 5:
                                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                                            else:
                                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                                            writer_thread.send_result(output)
                                    else:
                                        output = f"{hashkey}|{hexdigest}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                        writer_thread.send_result(output)

                        except Exception as e:
                            print(f"Error in worker {worker_id}: {e}")
                            # Mark worker as completed to avoid hanging
                            try:
                                worker_completed[worker_id] = True
                            except Exception:
                                # Ignore errors when trying to update shared objects during cleanup
                                pass

                    # Wait for progress monitoring to complete
                    monitor_thread.join()

            progress_bar.close()
        
        # Stop the writer thread for parallel processing
        if parallel:
            writer_thread.stop()

        else:
            # Sequential processing (original logic)
            if show_progress and HAS_TQDM and not parallel:
                progress_bar = tqdm(total=total_files, desc="Processing files", unit="file")
            else:
                progress_bar = None

            processed_count = 0

            start_dir = directory if directory else "."
            for subdir, dirs, files in os.walk(start_dir):
                if not files:
                    continue
                if subdir == ".uma":
                    continue
                if ".uma" in dirs:
                    dirs.remove(".uma")

                for filename in files:
                    if subdir == "." and (filename == hash_file or filename == file_writer.new_hash_file):
                        continue
                    full_filename = os.path.join(subdir, filename)

                    if os.path.islink(full_filename):
                        continue

                    file_stat = os.stat(full_filename)
                    file_size = file_stat.st_size
                    file_inode = file_stat.st_ino
                    file_mtime = file_stat.st_mtime

                    key = f"{full_filename}{file_size}{file_mtime}"
                    hashkey = _get_hash(key, algorithm)
                    filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")
                    subdir_encoded = (subdir.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                    if hashkey in cache:
                        if update:
                            cache_data = cache.pop(hashkey)
                            # Check if we can skip this file based on size and timestamp
                            if _can_skip_file(full_filename, cache_data, verbose, file_stat=file_stat):
                                # File is unchanged, just write the cached entry
                                if len(cache_data) == 5:
                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                                else:
                                    output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                                file_writer.outfile.write(output + "\n")
                            else:
                                # File has changed, recalculate hash
                                try:
                                    with open(full_filename, "rb") as f:
                                        hashsum, _ = calculate_hash(f, algorithm, show_progress=False)
                                    output = f"{hashkey}|{hashsum.hexdigest()}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                    file_writer.outfile.write(output + "\n")
                                except Exception as e:
                                    print(f"Error processing {full_filename}: {e}")
                                    if progress_bar:
                                        progress_bar.update(1)
                                    continue
                        else:
                            # Not update mode, just write cached entry
                            if len(cache_data) == 5:
                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                            else:
                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                            file_writer.outfile.write(output + "\n")
                    else:
                        try:
                            with open(full_filename, "rb") as f:
                                hashsum, _ = calculate_hash(f, algorithm, show_progress=False)
                        except Exception as e:
                            print(f"Error processing {full_filename}: {e}")
                            if progress_bar:
                                progress_bar.update(1)
                            continue

                        output = f"{hashkey}|{hashsum.hexdigest()}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                        file_writer.outfile.write(output + "\n")

                    processed_count += 1
                    if progress_bar:
                        progress_bar.update(1)

            if progress_bar:
                progress_bar.close()
            
            # Cleanup for sequential processing
            file_writer.__exit__(None, None, None)


def _load_hashfile(filename: str, destDict: Optional[Dict] = None,
                   cache_data: Optional[Dict] = None) -> Tuple[Dict, Optional[str]]:
    """Load hash file and populate destination dictionary. Returns (dict, algorithm)."""
    f = _asserted_open(filename, "r")

    if destDict is None:
        destDict = {}

    algorithm = None

    for line in f:
        line = line.rstrip()
        if not line:
            continue

        # Check for algorithm header
        if line.startswith("# Algorithm: "):
            algorithm = line[13:].strip()
            continue

        parts = line.split("|")
        if len(parts) == 6:
            # Old format without timestamp
            key, hashsum, dirname, filename, file_size, file_inode = parts
            fileinfo = (dirname, filename, file_size, file_inode, "0")  # Default timestamp for old entries
        elif len(parts) == 7:
            # New format with timestamp
            key, hashsum, dirname, filename, file_size, file_inode, file_mtime = parts
            fileinfo = (dirname, filename, file_size, file_inode, file_mtime)
        else:
            # Skip malformed lines
            continue

        if hashsum in destDict:
            if hashsum in repeated:
                repeated[hashsum].append(fileinfo)
            else:
                repeated[hashsum] = [destDict[hashsum], fileinfo]

        destDict[hashsum] = fileinfo

        if cache_data is not None:
            cache_data[key] = (hashsum, dirname, filename, file_size, file_inode, fileinfo[4])  # Include timestamp

    f.close()
    return destDict, algorithm


def compare(hashfile1: str, hashfile2: Optional[str] = None) -> None:
    """Compare two hash files and generate synchronization commands."""
    global output_file
    output_file = None

    hash_a, _ = _load_hashfile(hashfile1)
    hash_b = {}
    if hashfile2:
        hash_b, _ = _load_hashfile(hashfile2)

    commands = []
    mkdirs = set()
    rmdirs = set()
    only_in_1 = {}

    for hash_val in hash_a:
        fdata1 = hash_a[hash_val]
        fdata2 = None

        if hash_val in hash_b:
            fdata2 = hash_b[hash_val]
            hash_b.pop(hash_val)
        else:
            only_in_1[hash_val] = fdata1
            continue

        if fdata2[0] == fdata1[0] and fdata2[1] == fdata1[1]:
            # The files are the same
            continue
        else:
            print(f"Different: {fdata1} vs {fdata2}")
            if fdata1[0] == fdata2[0]:
                # Same directory, only filename changed
                commands.append(f"mv -v '{fdata1[0]}/{fdata1[1]}' '{fdata2[0]}/{fdata2[1]}'")
            else:
                # Directory changed and possibly filename too
                mkdirs.add(f"mkdir -pv '{fdata2[0]}'")
                if fdata1[0] != ".":
                    rmdirs.add(f"rmdir -v '{fdata1[0]}'")

                commands.append(f"mv -v '{fdata1[0]}/{fdata1[1]}' '{fdata2[0]}/{fdata2[1]}'")

    if hashfile2:
        if only_in_1:
            print(f"\n# Those files only exist in {hashfile1}")
            for item in _sorted_filenames(only_in_1):
                print(item)

        if hash_b:
            print(f"\n# Those files only exist in {hashfile2}")
            for item in _sorted_filenames(hash_b):
                print(item)

    if commands:
        tee(output_file, "\n# Commands to execute in console:")

        mkdirs_list = sorted(list(mkdirs))
        rmdirs_list = sorted(list(rmdirs), reverse=True)
        commands_sorted = sorted(commands)

        tee(output_file, "\n# mkdir statements.\n")
        for cmd in mkdirs_list:
            tee(output_file, cmd)

        tee(output_file, "\n# mv statements.\n")
        for cmd in commands_sorted:
            tee(output_file, cmd)

        tee(output_file, "\n# Directories possibly empty from now. Please check.\n")
        for cmd in rmdirs_list:
            tee(output_file, f"#{cmd}")

    if repeated:
        tee(output_file, """
# Those files are repeated. You can remove one or many of them if you wish.
# (Note: if the inode is the same, there is no space wasted)
""")
        filenames = []
        for item in repeated:
            filenames.extend([(l[0], l[1], item) for l in repeated[item]])

        filenames.sort()

        while filenames:
            item = filenames.pop(0)
            item_key = item[2]

            if item_key in repeated:
                repeated_files = repeated.pop(item_key)
                tee(output_file, f"\n# Key: {item_key} - Size: {repeated_files[0][2]}")

                commands_set = set()
                for repeated_file in repeated_files:
                    commands_set.add(f"# rm '{repeated_file[0]}/{repeated_file[1]}' # inode: {repeated_file[3]}")

                commands_list = sorted(list(commands_set))
                tee(output_file, "\n".join(commands_list))

    if output_file is not None:
        output_file.close()


def _sorted_filenames(filelist: Dict) -> List[str]:
    """Sort filenames from file list dictionary."""
    filenames = [os.path.join(filelist[item][0], filelist[item][1])
                 for item in filelist]
    filenames.sort()
    return filenames


def tee(o: Optional[Any], s: str) -> None:
    """Write to file and print to stdout."""
    global output_file
    if o is None:
        o = open(SCRIPT_FILENAME, "w")
        output_file = o

    o.write(s + "\n")
    print(s)


def _asserted_open(filename: str, mode: str) -> Any:
    """Open file with error handling."""
    try:
        return open(filename, mode)
    except IOError as e:
        print(f"I/O Error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Error: {e}")
        raise e


class HashFileWriter:
    """Context manager for writing hash files with proper cleanup."""
    
    def __init__(self, hash_file: str, update: bool, append: bool, algorithm: str):
        self.hash_file = hash_file
        self.update = update
        self.append = append
        self.algorithm = algorithm
        self.outfile = None
        self.new_hash_file = None
        self.results_written = 0
        self._cleanup_done = False


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
        self._writer_queue = queue.Queue()
        
    def start(self):
        """Start the writer thread."""
        self.new_hash_file = self.hash_file + ".new"
        
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
    
    def write_result(self, result_data: str):
        """Write a single result to the file."""
        if not self.outfile or self._cleanup_done:
            return
            
        try:
            self.outfile.write(result_data + "\n")
            self.results_written += 1
            
            # Flush periodically
            if self.results_written % self.write_frequency == 0:
                self.outfile.flush()
        except Exception as e:
            print(f"⚠️  Error writing result: {e}")
    
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
            except Exception as e:
                print(f"⚠️  Error in writer loop: {e}")
                continue
