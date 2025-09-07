#!/usr/bin/env python3

import os
import hashlib
import sys
import time
import queue
import configparser
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


def _collect_files(hash_file: str, collect_paths: bool = False) -> Union[int, List[Tuple[str, str, str]]]:
    """
    Collect files from current directory for processing.

    Args:
        hash_file: Name of the hash file to exclude
        collect_paths: If True, return list of (subdir, filename, full_filename)
                      If False, return count of files only

    Returns:
        If collect_paths is False: total file count (int)
        If collect_paths is True: list of file tuples
    """
    result = [] if collect_paths else 0

    for subdir, dirs, files in os.walk("."):
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
                    result.append((subdir, filename, full_filename))
                else:
                    result += 1

    return result


def _process_worker_batch(worker_files: List[Tuple[str, str, str]], algorithm: str, update: bool, append: bool, verbose: bool, worker_id: int, progress_queue: Optional['mp.Queue'] = None) -> List[Tuple]:
    """
    Process a batch of files for a single worker.
    This function runs in a separate process.
    """
    results = []

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
        # Note: We need to pass cache data to workers, but for now we'll handle this in the main process
        # This is a limitation of the current architecture - workers don't have access to cache
        
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

    # Signal completion
    if progress_queue:
        progress_queue.put(('done', worker_id))

    return results


def _get_hash(s: str, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Generate hash for a string using specified algorithm."""
    hash_func = SUPPORTED_ALGORITHMS.get(algorithm, hashlib.md5)
    return hash_func(s.encode("utf-8", "backslashreplace")).hexdigest()


def _can_skip_file(full_filename: str, cache_data: tuple, verbose: bool = False) -> bool:
    """
    Check if a file can be skipped based on size and modification time.
    
    Args:
        full_filename: Full path to the file
        cache_data: Tuple containing (hashsum, dirname, filename, file_size, file_inode, file_mtime)
        verbose: Whether to print skip messages
        
    Returns:
        True if file can be skipped, False otherwise
    """
    try:
        file_stat = os.stat(full_filename)
        current_size = file_stat.st_size
        current_mtime = file_stat.st_mtime
        
        # Handle both old format (5 fields) and new format (6 fields)
        if len(cache_data) == 5:
            cached_size, cached_mtime = int(cache_data[3]), 0  # Old format has no timestamp
        else:
            cached_size, cached_mtime = int(cache_data[3]), float(cache_data[5])
        
        # Skip if size and modification time match
        if current_size == cached_size and current_mtime == cached_mtime:
            if verbose:
                print(f"Skipping {os.path.basename(full_filename)} (unchanged)")
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
        
        # Send progress updates during processing for large files
        if progress_queue and verbose and readcount > 0 and readcount % 10 == 0:
            # Send periodic updates during file processing
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
                   verbose: bool = False) -> None:
    """Generate hash file for all files in current directory tree."""
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

    new_hash_file = hash_file + ".new"

    if update:
        # Create a new file with algorithm header and only needed hashes
        outfile = _asserted_open(new_hash_file, "w")
        outfile.write(f"# Algorithm: {algorithm}\n")
    else:
        if append:
            # Only appends new hashes
            outfile = _asserted_open(hash_file, "a")
        else:
            # Create a new file with algorithm header
            outfile = _asserted_open(hash_file, "w")
            outfile.write(f"# Algorithm: {algorithm}\n")

    # Count total files first for progress tracking (much faster than collecting all)
    total_files = _collect_files(hash_file, collect_paths=False)


    # Determine number of workers
    if parallel:
        if workers is None:
            workers = min(mp.cpu_count(), total_files) if total_files > 0 else 1
        else:
            workers = min(workers, total_files) if total_files > 0 else 1
    else:
        workers = 1

    # Use Rich progress bars for parallel processing with individual worker progress
    if parallel and show_progress and HAS_RICH:
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
            files_per_worker = total_files // workers
            extra_files = total_files % workers

            for i in range(workers):
                worker_total = files_per_worker
                if i < extra_files:
                    worker_total += 1
                task = progress.add_task(f"Worker {i+1}", total=worker_total, filename="")
                worker_tasks.append(task)

            # Create progress queues for real-time updates using Manager
            with mp.Manager() as manager:
                progress_queues = [manager.Queue() for _ in range(workers)]
                worker_completed = manager.list([False] * workers)

                # Process files on-the-fly with parallel workers using shared list
                with ProcessPoolExecutor(max_workers=workers) as executor:
                    # Collect all files for processing
                    all_files = _collect_files(hash_file, collect_paths=True)

                    # Distribute files among workers more evenly
                    files_per_worker = len(all_files) // workers
                    extra_files = len(all_files) % workers

                    worker_file_lists = []
                    start_idx = 0
                    for i in range(workers):
                        worker_files = files_per_worker
                        if i < extra_files:
                            worker_files += 1
                        end_idx = start_idx + worker_files
                        worker_file_lists.append(all_files[start_idx:end_idx])
                        start_idx = end_idx

                    # Submit all worker tasks at once
                    future_to_worker = {}
                    for worker_id, worker_files in enumerate(worker_file_lists):
                        if worker_files:  # Only create task if worker has files
                            future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, progress_queues[worker_id])
                            future_to_worker[future] = worker_id

                    # Function to monitor progress queues
                    def monitor_progress():
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
                                    elif message[0] == 'done':
                                        worker_completed[message[1]] = True
                                except queue.Empty:
                                    # Queue is empty, continue monitoring
                                    pass
                            time.sleep(0)  # Yield control to avoid busy waiting

                    # Start progress monitoring thread
                    monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                    monitor_thread.start()

                    # Process results as they complete
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
                                            outfile.write(output + "\n")
                                    else:
                                        output = f"{hashkey}|{hexdigest}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                        outfile.write(output + "\n")

                        except Exception as e:
                            print(f"Error in worker {worker_id}: {e}")
                            # Mark worker as completed to avoid hanging
                            worker_completed[worker_id] = True

                    # Wait for progress monitoring to complete
                    monitor_thread.join()

    elif parallel and show_progress and HAS_TQDM:
        # Fallback to tqdm for parallel processing with progress queues
        import threading
        progress_bar = tqdm(total=total_files, desc="Processing files", unit="file")

        # Collect all files for tqdm implementation
        all_files = _collect_files(hash_file, collect_paths=True)

        # Distribute files among workers
        files_per_worker = len(all_files) // workers
        extra_files = len(all_files) % workers

        worker_file_lists = []
        start_idx = 0
        for i in range(workers):
            worker_files = files_per_worker
            if i < extra_files:
                worker_files += 1
            end_idx = start_idx + worker_files
            worker_file_lists.append(all_files[start_idx:end_idx])
            start_idx = end_idx

        # Create progress queues for real-time updates using Manager
        with mp.Manager() as manager:
            progress_queues = [manager.Queue() for _ in range(workers)]
            worker_completed = manager.list([False] * workers)

            with ProcessPoolExecutor(max_workers=workers) as executor:
                # Submit worker tasks
                future_to_worker = {}
                for worker_id, worker_files in enumerate(worker_file_lists):
                    if worker_files:
                        future = executor.submit(_process_worker_batch, worker_files, algorithm, update, append, verbose, worker_id, progress_queues[worker_id])
                        future_to_worker[future] = worker_id

                # Function to monitor progress queues
                def monitor_progress():
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
                                elif message[0] == 'done':
                                    worker_completed[message[1]] = True
                            except queue.Empty:
                                # Queue is empty, continue monitoring
                                pass
                        time.sleep(0)  # Yield control to avoid busy waiting

                # Start progress monitoring thread
                monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                monitor_thread.start()

                # Process results
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
                                        outfile.write(output + "\n")
                                else:
                                    output = f"{hashkey}|{hexdigest}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                    outfile.write(output + "\n")

                    except Exception as e:
                        print(f"Error in worker {worker_id}: {e}")
                        # Mark worker as completed to avoid hanging
                        worker_completed[worker_id] = True

                # Wait for progress monitoring to complete
                monitor_thread.join()

        progress_bar.close()

    else:
        # Sequential processing (original logic)
        if show_progress and HAS_TQDM and not parallel:
            progress_bar = tqdm(total=total_files, desc="Processing files", unit="file")
        else:
            progress_bar = None

        processed_count = 0

        for subdir, dirs, files in os.walk("."):
            if not files:
                continue
            if subdir == ".uma":
                continue
            if ".uma" in dirs:
                dirs.remove(".uma")

            for filename in files:
                if subdir == "." and (filename == hash_file or filename == new_hash_file):
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
                        if _can_skip_file(full_filename, cache_data, verbose):
                            # File is unchanged, just write the cached entry
                            if len(cache_data) == 5:
                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|0"
                            else:
                                output = f"{hashkey}|{cache_data[0]}|{subdir_encoded}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}|{cache_data[5]}"
                            outfile.write(output + "\n")
                        else:
                            # File has changed, recalculate hash
                            try:
                                with open(full_filename, "rb") as f:
                                    hashsum, _ = calculate_hash(f, algorithm, show_progress=False)
                                output = f"{hashkey}|{hashsum.hexdigest()}|{subdir_encoded}|{filename_encoded}|{file_size}|{file_inode}|{file_mtime}"
                                outfile.write(output + "\n")
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
                        outfile.write(output + "\n")
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
                    outfile.write(output + "\n")

                processed_count += 1
                if progress_bar:
                    progress_bar.update(1)

        if progress_bar:
            progress_bar.close()

    outfile.close()
    if update:
        os.rename(new_hash_file, hash_file)
        if cache:
            print(f"{len(cache)} old cache entries cleaned.")
            for key in cache:
                cache_data = cache[key]
                print(f"{key} -> {cache_data[1]}/{cache_data[2]}")


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
