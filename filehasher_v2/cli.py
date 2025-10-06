"""
Command line interface for filehasher v2
"""

import sys
import signal
import argparse
from pathlib import Path
from typing import Optional, List

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

try:
    from .hash_algorithms import HashAlgorithm, BenchmarkRunner, get_algorithm_from_string
    from .file_scanner import FileScanner
    from .processor import HashProcessor
    from .hash_file import HashFileWriter
except ImportError:
    from hash_algorithms import HashAlgorithm, BenchmarkRunner, get_algorithm_from_string
    from file_scanner import FileScanner
    from processor import HashProcessor
    from hash_file import HashFileWriter

console = Console()


def _format_bytes(bytes_value: int) -> str:
    """Format bytes in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def generate_command(args):
    """Generate hash file for a directory."""
    try:
        # Parse algorithm
        algorithm = get_algorithm_from_string(args.algorithm)
        
        # Determine output file
        if args.output is None:
            args.output = args.directory.with_suffix('.hashes')
        
        # Check if output file exists and handle overwrite
        if args.output.exists() and not args.force:
            response = input(f"Output file '{args.output}' already exists. Overwrite? [y/N]: ")
            if response.lower() not in ['y', 'yes']:
                print("Operation cancelled.")
                return 1
        
        # Scan directory
        console.print(f"[blue]Scanning directory: {args.directory}[/blue]")
        scanner = FileScanner(str(args.directory), follow_symlinks=args.follow_symlinks)
        files, symlinks = scanner.scan_directory()
        
        if not files:
            console.print("[yellow]No files to process[/yellow]")
            return 0
        
        # Create processor
        processor = HashProcessor(algorithm=algorithm, num_workers=args.workers)
        
        # Process files
        console.print(f"[blue]Processing {len(files)} files with {args.workers} workers...[/blue]")
        success = processor.process_directory(
            str(args.directory), 
            str(args.output), 
            follow_symlinks=args.follow_symlinks,
            quiet=args.quiet
        )
        
        if success:
            # Show file info
            file_size = args.output.stat().st_size
            console.print(f"\n[green]✅ Hash file generated successfully![/green]")
            console.print(f"   File: {args.output}")
            console.print(f"   Size: {_format_bytes(file_size)}")
            console.print(f"   Algorithm: {algorithm.value.upper()}")
            return 0
        else:
            console.print("❌ Failed to generate hash file")
            return 1
    
    except KeyboardInterrupt:
        console.print("\n⚠️  Operation cancelled by user")
        return 1
    except Exception as e:
        console.print(f"❌ Error: {e}")
        return 1


def benchmark_command(args):
    """Benchmark hash algorithms."""
    try:
        # Run benchmark
        console.print(f"[blue]Benchmarking hash algorithms...[/blue]")
        console.print(f"[blue]Test file size: {args.size} MB[/blue]")
        
        benchmark = BenchmarkRunner()
        results = benchmark.benchmark_all(args.size)
        
        # Display results
        table = Table(title="Benchmark Results")
        table.add_column("Algorithm", style="cyan")
        table.add_column("Avg Time (s)", style="blue")
        table.add_column("Min Time (s)", style="green")
        table.add_column("Max Time (s)", style="yellow")
        table.add_column("Throughput (MB/s)", style="red")
        
        for result in results:
            table.add_row(
                result.algorithm.upper(),
                f"{result.avg_time:.4f}",
                f"{result.min_time:.4f}",
                f"{result.max_time:.4f}",
                f"{result.throughput_mbps:.2f}"
            )
        
        console.print(table)
        
        return 0
        
    except Exception as e:
        console.print(f"❌ Error: {e}")
        return 1


def create_parser():
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog='filehasher',
        description='FileHasher v2 - A modern file hashing tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  filehasher generate /path/to/directory
  filehasher generate /path/to/directory --algorithm sha256 --workers 8
  filehasher generate /path/to/directory --output custom.hashes --force
  filehasher benchmark --size 100 --algorithms md5,sha256,sha512
        """
    )
    
    parser.add_argument('--version', action='version', version='filehasher 2.0.0')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Generate command
    generate_parser = subparsers.add_parser('generate', help='Generate hash file for a directory')
    generate_parser.add_argument('directory', type=Path, help='Directory to hash')
    generate_parser.add_argument('--algorithm', '-a', default='md5', 
                               choices=['md5', 'sha1', 'sha256', 'sha512', 'blake2b', 'blake2s'],
                               help='Hash algorithm (default: md5)')
    generate_parser.add_argument('--output', '-o', type=Path, 
                               help='Output hash file path (default: directory.hashes)')
    generate_parser.add_argument('--workers', '-w', type=int, default=None,
                               help='Number of worker processes (default: CPU count)')
    generate_parser.add_argument('--quiet', '-q', action='store_true', 
                               help='Suppress progress output')
    generate_parser.add_argument('--follow-symlinks', action='store_true', 
                               help='Follow symbolic links')
    generate_parser.add_argument('--force', '-f', action='store_true', 
                               help='Overwrite output file without prompting')
    
    # Benchmark command
    benchmark_parser = subparsers.add_parser('benchmark', help='Benchmark hash algorithms')
    benchmark_parser.add_argument('--size', '-s', type=int, default=10,
                                help='Test file size in MB (default: 10)')
    benchmark_parser.add_argument('--algorithms', '-a', type=str,
                                help='Comma-separated list of algorithms to test')
    benchmark_parser.add_argument('--iterations', '-i', type=int, default=3,
                                help='Number of iterations per algorithm (default: 3)')
    
    return parser


def main():
    """Main entry point."""
    # Set up signal handlers
    def signal_handler(signum, frame):
        """Handle interrupt signals gracefully."""
        if signum == signal.SIGINT:
            console.print("\n[yellow]Received interrupt signal. Exiting...[/yellow]")
            sys.exit(1)
        elif signum == signal.SIGTERM:
            console.print("\n[yellow]Received termination signal. Exiting...[/yellow]")
            sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()
    
    # Check if no command provided
    if not args.command:
        parser.print_help()
        return 1
    
    # Route to appropriate command
    if args.command == 'generate':
        return generate_command(args)
    elif args.command == 'benchmark':
        return benchmark_command(args)
    else:
        console.print(f"❌ Unknown command: {args.command}")
        return 1


if __name__ == '__main__':
    sys.exit(main())