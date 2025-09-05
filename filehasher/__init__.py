#!/usr/bin/env python3

import os
import hashlib
import sys
import time
import configparser
from typing import Dict, List, Tuple, Optional, Any
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


def _process_file_worker(file_info: Tuple[str, str, str, str, str, bool]) -> Tuple[str, str, str, str, str, str]:
    """
    Worker function for parallel file processing.
    Returns (hashkey, hexdigest, subdir, filename, file_size, file_inode)
    """
    subdir, filename, full_filename, algorithm, update, append = file_info

    if os.path.islink(full_filename):
        return None  # Skip symlinks

    file_stat = os.stat(full_filename)
    file_size = file_stat.st_size
    file_inode = file_stat.st_ino

    key = f"{full_filename}{file_size}{file_stat.st_mtime}"
    hashkey = _get_hash(key, algorithm)

    try:
        with open(full_filename, "rb") as f:
            hashsum, _ = calculate_hash(f, algorithm, show_progress=False)
        hexdigest = hashsum.hexdigest()
    except Exception as e:
        print(f"Error processing {full_filename}: {e}")
        return None

    return hashkey, hexdigest, subdir, filename, str(file_size), str(file_inode)


def _get_hash(s: str, algorithm: str = DEFAULT_ALGORITHM) -> str:
    """Generate hash for a string using specified algorithm."""
    hash_func = SUPPORTED_ALGORITHMS.get(algorithm, hashlib.md5)
    return hash_func(s.encode("utf-8", "surrogateescape")).hexdigest()


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
                   parallel: bool = False, workers: Optional[int] = None) -> None:
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

    # Collect all files first for progress tracking
    all_files = []
    for subdir, dirs, files in os.walk("."):
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
            all_files.append((subdir, filename, full_filename))

    # Determine number of workers
    if parallel:
        if workers is None:
            workers = min(mp.cpu_count(), len(all_files)) if len(all_files) > 0 else 1
        else:
            workers = min(workers, len(all_files)) if len(all_files) > 0 else 1
    else:
        workers = 1

    # Use Rich progress bars for parallel processing
    if parallel and show_progress and HAS_RICH:
        console = Console()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            console=console,
            refresh_per_second=10,
        ) as progress:
            main_task = progress.add_task("Processing files", total=len(all_files))

            # Process files in parallel
            with ProcessPoolExecutor(max_workers=workers) as executor:
                # Submit all file processing tasks
                future_to_file = {}
                for subdir, filename, full_filename in all_files:
                    file_info = (subdir, filename, full_filename, algorithm, str(update), str(append))
                    future = executor.submit(_process_file_worker, file_info)
                    future_to_file[future] = (subdir, filename, full_filename)

                # Process completed tasks
                for future in as_completed(future_to_file):
                    subdir, filename, full_filename = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            hashkey, hexdigest, subdir, filename, file_size, file_inode = result
                            filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                            if hashkey in cache:
                                if update:
                                    cache_data = cache.pop(hashkey)
                                    output = f"{hashkey}|{cache_data[0]}|{subdir}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}"
                                    outfile.write(output + "\n")
                            else:
                                output = f"{hashkey}|{hexdigest}|{subdir}|{filename_encoded}|{file_size}|{file_inode}"
                                outfile.write(output + "\n")

                    except Exception as e:
                        print(f"Error processing {full_filename}: {e}")

                    progress.update(main_task, advance=1)

    elif parallel and show_progress and HAS_TQDM:
        # Fallback to tqdm for parallel processing
        progress_bar = tqdm(total=len(all_files), desc="Processing files", unit="file")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_file = {}
            for subdir, filename, full_filename in all_files:
                file_info = (subdir, filename, full_filename, algorithm, str(update), str(append))
                future = executor.submit(_process_file_worker, file_info)
                future_to_file[future] = (subdir, filename, full_filename)

            for future in as_completed(future_to_file):
                subdir, filename, full_filename = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        hashkey, hexdigest, subdir, filename, file_size, file_inode = result
                        filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                        if hashkey in cache:
                            if update:
                                cache_data = cache.pop(hashkey)
                                output = f"{hashkey}|{cache_data[0]}|{subdir}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}"
                                outfile.write(output + "\n")
                        else:
                            output = f"{hashkey}|{hexdigest}|{subdir}|{filename_encoded}|{file_size}|{file_inode}"
                            outfile.write(output + "\n")

                except Exception as e:
                    print(f"Error processing {full_filename}: {e}")

                progress_bar.update(1)

        progress_bar.close()

    else:
        # Sequential processing (original logic)
        if show_progress and HAS_TQDM and not parallel:
            progress_bar = tqdm(total=len(all_files), desc="Processing files", unit="file")
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

                key = f"{full_filename}{file_size}{file_stat.st_mtime}"
                hashkey = _get_hash(key, algorithm)
                filename_encoded = (filename.encode("utf-8", "backslashreplace")).decode("iso8859-1")

                if hashkey in cache:
                    if update:
                        cache_data = cache.pop(hashkey)
                        output = f"{hashkey}|{cache_data[0]}|{subdir}|{filename_encoded}|{cache_data[3]}|{cache_data[4]}"
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

                    output = f"{hashkey}|{hashsum.hexdigest()}|{subdir}|{filename_encoded}|{file_size}|{file_inode}"
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

        key, hashsum, dirname, filename, file_size, file_inode = line.split("|")
        fileinfo = (dirname, filename, file_size, file_inode)

        if hashsum in destDict:
            if hashsum in repeated:
                repeated[hashsum].append(fileinfo)
            else:
                repeated[hashsum] = [destDict[hashsum], fileinfo]

        destDict[hashsum] = fileinfo

        if cache_data is not None:
            cache_data[key] = (hashsum, dirname, filename, file_size, file_inode)

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
