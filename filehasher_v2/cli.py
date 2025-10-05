"""
Command line interface for filehasher
"""

import multiprocessing
import sys
import signal
import time
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from .hash_algorithms import ALGORITHMS, get_algorithm, benchmark_algorithms, create_test_file, cleanup_test_file
from .file_scanner import FileScanner
from .processor import FileProcessor
from .hash_file import HashFile, HashEntry


console = Console()


@click.group()
@click.version_option(version="2.0.0", prog_name="filehasher")
def cli():
    """Filehasher - A modern file hashing and comparison tool"""
    pass


@cli.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--algorithm', '-a', default='md5', 
              help=f'Hash algorithm to use. Available: {", ".join(ALGORITHMS.keys())}')
@click.option('--output', '-o', type=click.Path(path_type=Path),
              help='Output hash file path (default: directory_name.hashes)')
@click.option('--processes', '-m', default='1',
              help='Number of processes to use (default: 1, use "auto" for CPU count)')
@click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
@click.option('--follow-symlinks', is_flag=True, help='Follow symbolic links')
@click.option('--update', '-u', is_flag=True, help='Update existing hash file, only hash changed files')
def generate(directory: Path, algorithm: str, output: Optional[Path], 
             processes: str, quiet: bool, follow_symlinks: bool, update: bool):
    """Generate hash file for a directory"""
    
    # Parse processes option
    if processes == 'auto':
        num_processes = multiprocessing.cpu_count()
    else:
        try:
            num_processes = int(processes)
            if num_processes < 1:
                raise click.BadParameter("Number of processes must be at least 1")
        except ValueError:
            raise click.BadParameter("Processes must be a number or 'auto'")
    
    # Set default output file
    if output is None:
        output = directory.name + '.hashes'
    
    # Get hash algorithm
    try:
        hash_algorithm = get_algorithm(algorithm)
    except ValueError as e:
        raise click.BadParameter(str(e))
    
    console.print(f"[bold green]Generating hashes for:[/bold green] {directory}")
    console.print(f"[bold blue]Algorithm:[/bold blue] {hash_algorithm.name}")
    console.print(f"[bold blue]Output:[/bold blue] {output}")
    console.print(f"[bold blue]Processes:[/bold blue] {num_processes}")
    if update:
        console.print(f"[bold blue]Mode:[/bold blue] Update existing hash file")
    
    # Scan directory
    scanner = FileScanner(directory, follow_symlinks=follow_symlinks)
    
    try:
        files = scanner.scan_directory()
        
        # Handle update mode
        files_to_process = files
        if update and Path(output).exists():
            # Load existing hash file
            existing_hash_file = HashFile(Path(output))
            existing_hash_file.read_entries()
            
            # Filter files that need updating
            files_to_process = [f for f in files if existing_hash_file.needs_update(f)]
            
            if not files_to_process:
                console.print("[green]No files need updating![/green]")
                return
            
            console.print(f"[bold green]Found {len(files)} total files, {len(files_to_process)} need updating[/bold green]")
        else:
            console.print(f"[bold green]Found {len(files)} files to process[/bold green]")
        
        if not files_to_process:
            console.print("[yellow]No files found to process[/yellow]")
            return
        
        # Load balance files across processes
        file_batches = scanner.load_balance_files(files_to_process, num_processes)
        
        # Process files
        processor = FileProcessor(hash_algorithm, num_processes, show_progress=not quiet)
        results = processor.process_files(file_batches)
        
        # Create or update hash file
        hash_file = HashFile(Path(output))
        
        if update and Path(output).exists():
            # Update mode: load existing entries and update/add new ones
            hash_file.read_entries()
            
            # Preserve metadata from existing file
            if not hash_file.metadata:
                hash_file.write_metadata_header(directory)
            
            # Remove old entries for files we're updating
            updated_paths = {r.file_info.relative_path for r in results}
            hash_file.entries = [e for e in hash_file.entries 
                               if Path(e.directory) / e.filename not in updated_paths]
        else:
            # New file: write header
            hash_file.write_header(directory)
        
        # Add new/updated entries
        for result in results:
            if result.success:
                entry = HashEntry(
                    file_hash=result.file_hash,
                    other_hash=result.other_hash,
                    directory=str(result.file_info.relative_path.parent) if result.file_info.relative_path.parent != Path('.') else '.',
                    filename=result.file_info.relative_path.name,
                    file_size=result.file_info.size,
                    inode=result.file_info.inode,
                    mtime=result.file_info.mtime,
                    is_symlink=result.file_info.is_symlink
                )
                hash_file.add_entry(entry, algorithm, write_to_file=not update)
            else:
                # Add symlinks as commented entries
                if result.file_info.is_symlink:
                    entry = HashEntry(
                        file_hash='',
                        other_hash='',
                        directory=str(result.file_info.relative_path.parent) if result.file_info.relative_path.parent != Path('.') else '.',
                        filename=result.file_info.relative_path.name,
                        file_size=result.file_info.size,
                        inode=result.file_info.inode,
                        mtime=result.file_info.mtime,
                        is_symlink=True
                    )
                    hash_file.add_entry(entry, write_to_file=not update)
        
        # Rewrite the entire file in update mode
        if update and Path(output).exists():
            hash_file.write_all_entries(algorithm)
        
        # Print summary
        processor.print_summary(results)
        
        # Print timing statistics
        processor.print_timing_statistics(results)
        
        action = "updated" if update else "created"
        console.print(f"[bold green]Hash file {action}:[/bold green] {output}")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@cli.command()
@click.argument('hash_file', type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(path_type=Path),
              default='cleanup_duplicates.sh',
              help='Output script path (default: cleanup_duplicates.sh)')
def duplicates(hash_file: Path, output: Path):
    """Find duplicates within a hash file and generate cleanup script"""
    
    console.print(f"[bold green]Finding duplicates in:[/bold green] {hash_file}")
    
    try:
        # Read hash file
        file_hash = HashFile(Path(hash_file))
        entries = file_hash.read_entries()
        
        if not entries:
            console.print("[yellow]No entries found in hash file[/yellow]")
            return
        
        # Find duplicates
        duplicates = file_hash.find_duplicates()
        
        if not duplicates:
            console.print("[green]No duplicates found![/green]")
            return
        
        console.print(f"[bold blue]Found {len(duplicates)} duplicate groups[/bold blue]")
        
        # Generate cleanup script
        with open(output, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Cleanup script for duplicate files\n")
            f.write("# Uncomment the lines you want to execute\n\n")
            
            for i, (hash_value, duplicate_entries) in enumerate(duplicates.items(), 1):
                f.write(f"# Duplicate group {i} (hash: {hash_value})\n")
                
                # Keep the first file, remove the rest
                keep_file = duplicate_entries[0]
                f.write(f"# KEEP: {keep_file.directory}/{keep_file.filename}\n")
                
                for entry in duplicate_entries[1:]:
                    full_path = Path(entry.directory) / entry.filename
                    f.write(f"# rm '{full_path}'\n")
                
                f.write("\n")
        
        console.print(f"[bold green]Cleanup script generated:[/bold green] {output}")
        console.print("[yellow]Review the script and uncomment the lines you want to execute[/yellow]")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@cli.command()
@click.argument('hash_file1', type=click.Path(exists=True, path_type=Path))
@click.argument('hash_file2', type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(path_type=Path),
              default='sync_directories.sh',
              help='Output script path (default: sync_directories.sh)')
def compare(hash_file1: Path, hash_file2: Path, output: Path):
    """Compare two hash files and generate sync script"""
    
    console.print(f"[bold green]Comparing hash files:[/bold green]")
    console.print(f"  File 1: {hash_file1}")
    console.print(f"  File 2: {hash_file2}")
    
    try:
        # Read hash files
        file1 = HashFile(Path(hash_file1))
        file2 = HashFile(Path(hash_file2))
        
        entries1 = file1.read_entries()
        entries2 = file2.read_entries()
        
        if not entries1 or not entries2:
            console.print("[yellow]One or both hash files are empty[/yellow]")
            return
        
        # Find matches
        matches = file1.compare_with(file2)
        
        if not matches:
            console.print("[yellow]No matching files found between the two hash files[/yellow]")
            return
        
        console.print(f"[bold blue]Found {len(matches)} matching files[/bold blue]")
        
        # Generate sync script
        with open(output, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Sync script for matching files\n")
            f.write("# Uncomment the lines you want to execute\n\n")
            
            for i, (hash_value, match_entries) in enumerate(matches.items(), 1):
                f.write(f"# Match group {i} (hash: {hash_value})\n")
                
                # Group entries by source file
                file1_entries = [e for e in match_entries if e in entries1]
                file2_entries = [e for e in match_entries if e in entries2]
                
                for entry1 in file1_entries:
                    for entry2 in file2_entries:
                        if entry1.filename != entry2.filename:
                            source_path = Path(entry1.directory) / entry1.filename
                            target_path = Path(entry2.directory) / entry2.filename
                            f.write(f"# mv '{source_path}' '{target_path}'\n")
                
                f.write("\n")
        
        console.print(f"[bold green]Sync script generated:[/bold green] {output}")
        console.print("[yellow]Review the script and uncomment the lines you want to execute[/yellow]")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@cli.command()
@click.option('--size', '-s', default=10, help='Test file size in MB (default: 10)')
@click.option('--algorithms', '-a', help='Comma-separated list of algorithms to test')
def benchmark(size: int, algorithms: Optional[str]):
    """Benchmark different hash algorithms"""
    
    console.print(f"[bold green]Benchmarking hash algorithms[/bold green]")
    console.print(f"[bold blue]Test file size:[/bold blue] {size} MB")
    
    # Parse algorithms
    if algorithms:
        algo_list = [a.strip() for a in algorithms.split(',')]
        invalid_algos = [a for a in algo_list if a not in ALGORITHMS]
        if invalid_algos:
            raise click.BadParameter(f"Invalid algorithms: {', '.join(invalid_algos)}")
    else:
        algo_list = list(ALGORITHMS.keys())
    
    try:
        # Create test file
        console.print("[bold blue]Creating test file...[/bold blue]")
        test_file = create_test_file(size)
        
        try:
            # Run benchmarks
            console.print("[bold blue]Running benchmarks...[/bold blue]")
            results = benchmark_algorithms(test_file, algo_list)
            
            # Display results
            table = Table(title="Hash Algorithm Benchmarks")
            table.add_column("Algorithm", style="cyan")
            table.add_column("Time (seconds)", style="green")
            table.add_column("Speed (MB/s)", style="yellow")
            
            for algo, time_taken in sorted(results.items(), key=lambda x: x[1]):
                speed = size / time_taken if time_taken > 0 else 0
                table.add_row(algo.upper(), f"{time_taken:.3f}", f"{speed:.1f}")
            
            console.print(table)
            
        finally:
            # Clean up test file
            cleanup_test_file(test_file)
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli()
