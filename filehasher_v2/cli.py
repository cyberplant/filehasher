"""
Command line interface for filehasher v2
"""

import sys
import signal
from pathlib import Path
from typing import Optional, List
import logging

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

try:
    from .hash_algorithms import HashAlgorithm, HashBenchmark, get_algorithm_from_string
    from .file_scanner import FileScanner
    from .processor import MultiprocessHashProcessor
    from .hash_file import HashFile, HashEntry
except ImportError:
    from hash_algorithms import HashAlgorithm, HashBenchmark, get_algorithm_from_string
    from file_scanner import FileScanner
    from processor import MultiprocessHashProcessor
    from hash_file import HashFile, HashEntry

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

console = Console()


@click.group()
@click.version_option(version="2.0.0", prog_name="filehasher")
def cli():
    """FileHasher v2 - A modern file hashing and comparison tool"""
    pass


@cli.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--algorithm', '-a', default='md5', 
              help='Hash algorithm (md5, sha1, sha256, sha512, blake2b, blake2s)')
@click.option('--output', '-o', type=click.Path(path_type=Path),
              help='Output hash file path (default: directory.hashes)')
@click.option('--workers', '-w', type=int, default=None,
              help='Number of worker processes (default: CPU count)')
@click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
@click.option('--follow-symlinks', is_flag=True, help='Follow symbolic links')
@click.option('--update', '-u', is_flag=True, 
              help='Update existing hash file, only hash changed files')
def generate(directory: Path, algorithm: str, output: Optional[Path], 
            workers: Optional[int], quiet: bool, follow_symlinks: bool, update: bool):
    """Generate hash file for a directory"""
    
    try:
        # Parse algorithm
        hash_algorithm = get_algorithm_from_string(algorithm)
        
        # Determine output file
        if output is None:
            output = directory.with_suffix('.hashes')
        
        # Check if updating existing file
        hash_file = HashFile(output)
        existing_entries = {}
        if update and output.exists():
            console.print(f"[blue]Reading existing hash file: {output}[/blue]")
            hash_file.read()  # Read the hash file first
            existing_entries = hash_file.get_file_info_map()
        
        # Scan directory
        console.print(f"[blue]Scanning directory: {directory}[/blue]")
        scanner = FileScanner(follow_symlinks=follow_symlinks)
        
        if update and existing_entries:
            # Use efficient update scanning - only scan files that need updating
            files_to_process = scanner.scan_directory_for_update(directory, existing_entries)
            console.print(f"[blue]Found {len(files_to_process)} files to update[/blue]")
        else:
            # Full directory scan for new hash file
            files_to_process = scanner.scan_directory(directory)
        
        if not files_to_process:
            console.print("[yellow]No files to process[/yellow]")
            return
        
        # Distribute files across workers
        if workers is None:
            workers = 1  # Start with single worker for simplicity
        
        file_lists = scanner.distribute_files_for_processing(workers)
        
        # Process files
        processor = MultiprocessHashProcessor(hash_algorithm, workers, quiet)
        results = processor.process_files(file_lists)
        
        if not results:
            console.print("[red]No files were processed successfully[/red]")
            return
        
        # Convert results to hash entries
        entries = []
        for result in results:
            if result.success:
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
        
        # Write hash file
        console.print(f"[blue]Writing hash file: {output}[/blue]")
        hash_file.write(entries, directory, hash_algorithm, update_mode=update)
        
        # Display statistics
        if not quiet:
            processor.display_statistics()
        
        console.print(f"[green]✓ Hash file generated: {output}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error in generate command")
        sys.exit(1)


@cli.command()
@click.argument('hash_file', type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(path_type=Path),
              default='cleanup_duplicates.sh',
              help='Output script path (default: cleanup_duplicates.sh)')
def duplicates(hash_file: Path, output: Path):
    """Find duplicate files in a hash file"""
    
    try:
        # Read hash file
        console.print(f"[blue]Reading hash file: {hash_file}[/blue]")
        hash_file_obj = HashFile(hash_file)
        entries = hash_file_obj.read()
        
        # Find duplicates
        duplicates_dict = hash_file_obj.get_duplicates()
        
        if not duplicates_dict:
            console.print("[green]No duplicate files found[/green]")
            return
        
        # Generate cleanup script
        console.print(f"[blue]Generating cleanup script: {output}[/blue]")
        
        with open(output, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Cleanup script for duplicate files\n")
            f.write("# Uncomment the lines you want to execute\n\n")
            
            for i, (hash_val, duplicate_entries) in enumerate(duplicates_dict.items(), 1):
                f.write(f"# Duplicate group {i} (hash: {hash_val[:16]}...)\n")
                
                for j, entry in enumerate(duplicate_entries):
                    f.write(f"# rm '{entry.relative_path}'\n")
                
                f.write("\n")
        
        # Make script executable
        output.chmod(0o755)
        
        # Display summary
        total_duplicates = sum(len(group) for group in duplicates_dict.values())
        total_groups = len(duplicates_dict)
        
        table = Table(title="Duplicate Files Found")
        table.add_column("Group", style="cyan")
        table.add_column("Files", style="blue")
        table.add_column("Size", style="green")
        table.add_column("Hash", style="yellow")
        
        for i, (hash_val, group) in enumerate(duplicates_dict.items(), 1):
            total_size = sum(entry.size for entry in group)
            table.add_row(
                str(i),
                str(len(group)),
                f"{total_size:,} bytes",
                hash_val[:16] + "..."
            )
        
        console.print(table)
        console.print(f"[green]✓ Cleanup script generated: {output}[/green]")
        console.print(f"[blue]Found {total_groups} duplicate groups with {total_duplicates} files total[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error in duplicates command")
        sys.exit(1)


@cli.command()
@click.argument('file1', type=click.Path(exists=True, path_type=Path))
@click.argument('file2', type=click.Path(exists=True, path_type=Path))
@click.option('--output', '-o', type=click.Path(path_type=Path),
              default='sync_directories.sh',
              help='Output script path (default: sync_directories.sh)')
def compare(file1: Path, file2: Path, output: Path):
    """Compare two hash files and generate sync script"""
    
    try:
        # Read hash files
        console.print(f"[blue]Reading hash files...[/blue]")
        hash_file1 = HashFile(file1)
        hash_file2 = HashFile(file2)
        
        entries1 = hash_file1.read()
        entries2 = hash_file2.read()
        
        # Compare files
        comparison = hash_file1.compare_hash_files(hash_file2)
        
        # Generate sync script
        console.print(f"[blue]Generating sync script: {output}[/blue]")
        
        with open(output, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Sync script for matching files\n")
            f.write("# Uncomment the lines you want to execute\n\n")
            
            # Files only in file1 (to be removed or moved)
            if comparison['only_in_file1']:
                f.write("# Files only in first directory (consider removing)\n")
                for entry in comparison['only_in_file1']:
                    f.write(f"# rm '{entry.relative_path}'\n")
                f.write("\n")
            
            # Files only in file2 (to be copied)
            if comparison['only_in_file2']:
                f.write("# Files only in second directory (consider copying)\n")
                for entry in comparison['only_in_file2']:
                    f.write(f"# cp '{entry.relative_path}' 'destination/'\n")
                f.write("\n")
            
            # Matching files with different paths
            if comparison['matching_hashes']:
                f.write("# Files with same content but different names\n")
                for match in comparison['matching_hashes']:
                    f.write(f"# mv '{match['file1_path']}' '{match['file2_path']}'\n")
                f.write("\n")
        
        # Make script executable
        output.chmod(0o755)
        
        # Display comparison summary
        table = Table(title="Comparison Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="blue")
        
        table.add_row("Files in first directory", str(comparison['total_file1']))
        table.add_row("Files in second directory", str(comparison['total_file2']))
        table.add_row("Common hashes", str(comparison['common_hashes']))
        table.add_row("Only in first", str(len(comparison['only_in_file1'])))
        table.add_row("Only in second", str(len(comparison['only_in_file2'])))
        table.add_row("Same content, different names", str(len(comparison['matching_hashes'])))
        
        console.print(table)
        console.print(f"[green]✓ Sync script generated: {output}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error in compare command")
        sys.exit(1)


@cli.command()
@click.option('--size', '-s', type=int, default=10,
              help='Test file size in MB (default: 10)')
@click.option('--algorithms', '-a', default=None,
              help='Comma-separated list of algorithms to test')
@click.option('--iterations', '-i', type=int, default=3,
              help='Number of iterations per algorithm (default: 3)')
def benchmark(size: int, algorithms: Optional[str], iterations: int):
    """Benchmark hash algorithms on your machine"""
    
    try:
        # Parse algorithms
        if algorithms:
            algorithm_list = [get_algorithm_from_string(alg.strip()) 
                            for alg in algorithms.split(',')]
        else:
            algorithm_list = None
        
        # Run benchmark
        console.print(f"[blue]Benchmarking hash algorithms...[/blue]")
        console.print(f"[blue]Test file size: {size} MB, Iterations: {iterations}[/blue]")
        
        benchmark = HashBenchmark(size)
        results = benchmark.benchmark_all(algorithm_list, iterations)
        
        # Display results
        table = Table(title="Benchmark Results")
        table.add_column("Algorithm", style="cyan")
        table.add_column("Avg Time (s)", style="blue")
        table.add_column("Min Time (s)", style="green")
        table.add_column("Max Time (s)", style="yellow")
        table.add_column("Throughput (MB/s)", style="red")
        
        for result in results:
            table.add_row(
                result['algorithm'].upper(),
                f"{result['avg_time']:.4f}",
                f"{result['min_time']:.4f}",
                f"{result['max_time']:.4f}",
                f"{result['throughput_mb_s']:.2f}"
            )
        
        console.print(table)
        
        # Show recommendation
        recommendation = benchmark.get_recommendation(results)
        console.print(f"\n[green]Recommendation: {recommendation}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error in benchmark command")
        sys.exit(1)


@cli.command()
@click.argument('hash_file', type=click.Path(exists=True, path_type=Path))
def info(hash_file: Path):
    """Display information about a hash file"""
    
    try:
        # Read hash file
        hash_file_obj = HashFile(hash_file)
        entries = hash_file_obj.read()
        stats = hash_file_obj.get_statistics()
        
        # Display header info
        if hash_file_obj.header:
            header_table = Table(title="Hash File Information")
            header_table.add_column("Property", style="cyan")
            header_table.add_column("Value", style="blue")
            
            header_table.add_row("File", str(hash_file))
            header_table.add_row("Version", hash_file_obj.header.version)
            header_table.add_row("Machine", hash_file_obj.header.machine)
            header_table.add_row("Base Directory", hash_file_obj.header.base_directory)
            header_table.add_row("User", hash_file_obj.header.user)
            header_table.add_row("Generated", hash_file_obj.header.generated.strftime('%Y-%m-%d %H:%M:%S'))
            header_table.add_row("Algorithm", stats['algorithm'])
            
            console.print(header_table)
        
        # Display statistics
        stats_table = Table(title="File Statistics")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Count", style="blue")
        
        stats_table.add_row("Total Entries", str(stats['total_entries']))
        stats_table.add_row("Regular Files", str(stats['regular_files']))
        stats_table.add_row("Symbolic Links", str(stats['symlinks']))
        stats_table.add_row("Total Size", f"{stats['total_size']:,} bytes")
        stats_table.add_row("Duplicate Groups", str(stats['duplicate_groups']))
        stats_table.add_row("Duplicate Files", str(stats['duplicate_files']))
        
        console.print(stats_table)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Error in info command")
        sys.exit(1)


def main():
    """Main entry point"""
    # Set up signal handlers only for the main process
    def signal_handler(signum, frame):
        """Handle interrupt signals gracefully"""
        if signum == signal.SIGINT:
            console.print("\n[yellow]Received interrupt signal. Exiting...[/yellow]")
            sys.exit(1)
        elif signum == signal.SIGTERM:
            console.print("\n[yellow]Received termination signal. Exiting...[/yellow]")
            sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        logger.exception("Unexpected error in main")
        sys.exit(1)


if __name__ == '__main__':
    main()
