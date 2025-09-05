#!/usr/bin/env python3

import filehasher
import sys
import argparse
from typing import List
from filehasher.version import __version__


def main():
    parser = argparse.ArgumentParser(
        description='File Hasher - Generate and compare file hashes with multiple algorithms.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --version
  %(prog)s --generate --algorithm sha256
  %(prog)s --benchmark
  %(prog)s --compare other.hashes
  %(prog)s --update --algorithm md5
  %(prog)s --generate --parallel --workers 4
  %(prog)s --generate --parallel --algorithm sha256
  %(prog)s --generate --parallel --verbose
  %(prog)s --generate --parallel --workers 2 --verbose

Supported algorithms: md5, sha1, sha256, sha512, blake2b, blake2s
        """
    )

    # File operations
    parser.add_argument('--generate', '-g', action='store_true',
                        dest="generate",
                        help="Generate hashes (remove hashfile if exists)")

    parser.add_argument('--append', '-a', action='store_true',
                        dest="append",
                        help="Append hashes to hashfile")

    parser.add_argument('--update', '-u', action='store_true',
                        dest="update",
                        help="Update hashfile (clean old entries and append new)")

    # Algorithm selection
    parser.add_argument('--algorithm', '-A', default='md5',
                        choices=list(filehasher.SUPPORTED_ALGORITHMS.keys()),
                        help="Hash algorithm to use (default: %(default)s)")

    # Benchmarking
    parser.add_argument('--benchmark', '-b', action='store_true',
                        dest="benchmark",
                        help="Benchmark all supported hash algorithms")

    parser.add_argument('--benchmark-file', dest="benchmark_file",
                        help="Use specific file for benchmarking instead of sample data")

    parser.add_argument('--benchmark-iterations', type=int, default=3,
                        dest="benchmark_iterations",
                        help="Number of iterations for benchmarking (default: %(default)s)")

    parser.add_argument('--benchmark-algorithms', nargs='+',
                        dest="benchmark_algorithms",
                        help="Specific algorithms to benchmark (default: all)")

    # Comparison
    parser.add_argument('--compare', '-c', nargs='?',
                        dest="compare", default=False,
                        help="Compare hashes from hashfiles. You can use this "
                             "command to check duplicates.")

    # Progress control
    parser.add_argument('--quiet', '-q', action='store_true',
                        dest="quiet",
                        help="Suppress progress output")

    # Parallel processing
    parser.add_argument('--parallel', '-P', action='store_true',
                        dest="parallel",
                        help="Process files in parallel using multiple workers")

    parser.add_argument('--workers', type=int, default=None,
                        dest="workers",
                        help="Number of parallel workers (default: CPU count)")

    parser.add_argument('--verbose', '-v', action='store_true',
                        dest="verbose",
                        help="Show detailed progress including filenames being processed")

    parser.add_argument('--version', '-V', action='version', version=__version__,
                        help="Show version information")

    parser.add_argument('hashfile', default='.hashes', nargs='?',
                        help="Hashes file. Default filename: %(default)s")

    args = parser.parse_args()

    # Load configuration
    try:
        config = filehasher.load_config()
    except Exception:
        config = {
            'default_algorithm': 'md5',
            'benchmark_iterations': 3,
            'quiet': False,
        }

    # Use config defaults if not specified
    if not hasattr(args, 'algorithm') or args.algorithm == 'md5':
        args.algorithm = config['default_algorithm']
    if not args.benchmark_iterations:
        args.benchmark_iterations = config['benchmark_iterations']
    if not args.quiet:
        args.quiet = config['quiet']

    # Handle benchmarking first
    if args.benchmark:
        print("Running hash algorithm benchmark...")
        results = filehasher.benchmark_algorithms(
            test_file=args.benchmark_file,
            algorithms=args.benchmark_algorithms,
            iterations=args.benchmark_iterations
        )

        print("\nBenchmark Results:")
        print("-" * 80)
        print("Algorithm     Avg Time (s)    Min Time (s)    Max Time (s)    Throughput (MB/s)")
        print("-" * 80)

        for algorithm, metrics in sorted(results.items(),
                                        key=lambda x: x[1]['throughput_mb_per_sec'],
                                        reverse=True):
            print(f"{algorithm:<12} {metrics['average_time']:>12.4f} {metrics['min_time']:>12.4f} {metrics['max_time']:>12.4f} {metrics['throughput_mb_per_sec']:>16.2f}")
        print("-" * 80)
        sys.exit(0)

    # Handle file operations
    show_progress = not args.quiet

    if args.generate:
        print(f"Generating {args.algorithm} hashes...")
        filehasher.generate_hashes(
            args.hashfile,
            algorithm=args.algorithm,
            show_progress=show_progress,
            parallel=args.parallel,
            workers=args.workers,
            verbose=args.verbose
        )
    elif args.append:
        print(f"Appending {args.algorithm} hashes...")
        filehasher.generate_hashes(
            args.hashfile,
            append=True,
            algorithm=args.algorithm,
            show_progress=show_progress,
            parallel=args.parallel,
            workers=args.workers,
            verbose=args.verbose
        )
    elif args.update:
        print(f"Updating {args.algorithm} hashes...")
        filehasher.generate_hashes(
            args.hashfile,
            update=True,
            algorithm=args.algorithm,
            show_progress=show_progress,
            parallel=args.parallel,
            workers=args.workers,
            verbose=args.verbose
        )
    elif args.compare:
        filehasher.compare(args.hashfile, args.compare)
    else:
        parser.print_help()

    sys.exit(0)


if __name__ == "__main__":
    main()
