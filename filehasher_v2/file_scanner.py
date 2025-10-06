"""
File scanner for directory traversal and load balancing.
"""

import os
import stat
from typing import List, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileInfo:
    """Information about a file to be processed."""
    path: str
    relative_path: str
    size: int
    inode: int
    mtime: int
    is_symlink: bool = False


class FileScanner:
    """Scans directories and provides load-balanced file distribution."""
    
    def __init__(self, base_directory: str, follow_symlinks: bool = False):
        self.base_directory = Path(base_directory).resolve()
        self.follow_symlinks = follow_symlinks
        self._files: List[FileInfo] = []
        self._symlinks: List[FileInfo] = []
    
    def scan_directory(self) -> Tuple[List[FileInfo], List[FileInfo]]:
        """
        Scan the base directory and collect file information.
        
        Returns:
            Tuple of (regular_files, symlinks)
        """
        self._files.clear()
        self._symlinks.clear()
        
        try:
            self._scan_recursive(self.base_directory, Path("."))
        except (OSError, PermissionError) as e:
            print(f"Warning: Error scanning directory {self.base_directory}: {e}")
        
        return self._files.copy(), self._symlinks.copy()
    
    def _scan_recursive(self, current_path: Path, relative_path: Path):
        """
        Recursively scan directory for files.
        
        Args:
            current_path: Absolute path to current directory
            relative_path: Relative path from base directory
        """
        try:
            for item in current_path.iterdir():
                try:
                    stat_info = item.stat()
                    item_relative = relative_path / item.name
                    
                    if item.is_symlink():
                        # Handle symlinks
                        symlink_info = FileInfo(
                            path=str(item),
                            relative_path=str(item_relative),
                            size=0,  # Symlinks have no size
                            inode=stat_info.st_ino,
                            mtime=int(stat_info.st_mtime),
                            is_symlink=True
                        )
                        self._symlinks.append(symlink_info)
                        
                        # Follow symlink if requested and it's a directory
                        if self.follow_symlinks and item.is_dir():
                            try:
                                self._scan_recursive(item, item_relative)
                            except (OSError, PermissionError):
                                pass  # Skip if can't follow symlink
                    
                    elif item.is_file():
                        # Regular file
                        file_info = FileInfo(
                            path=str(item),
                            relative_path=str(item_relative),
                            size=stat_info.st_size,
                            inode=stat_info.st_ino,
                            mtime=int(stat_info.st_mtime),
                            is_symlink=False
                        )
                        self._files.append(file_info)
                    
                    elif item.is_dir():
                        # Recursively scan subdirectory
                        self._scan_recursive(item, item_relative)
                
                except (OSError, PermissionError) as e:
                    # Skip files/directories we can't access
                    print(f"Warning: Cannot access {item}: {e}")
                    continue
        
        except (OSError, PermissionError) as e:
            print(f"Warning: Cannot access directory {current_path}: {e}")
    
    def get_total_size(self) -> int:
        """Get total size of all files in bytes."""
        return sum(file.size for file in self._files)
    
    def get_file_count(self) -> int:
        """Get total number of files."""
        return len(self._files)
    
    def distribute_files(self, num_workers: int) -> List[List[FileInfo]]:
        """
        Distribute files across workers based on total bytes, not file count.
        
        Args:
            num_workers: Number of worker processes
            
        Returns:
            List of file lists, one for each worker
        """
        if not self._files:
            return [[] for _ in range(num_workers)]
        
        if num_workers == 1:
            return [self._files.copy()]
        
        # Sort files by size (largest first) for better load balancing
        sorted_files = sorted(self._files, key=lambda f: f.size, reverse=True)
        
        # Initialize worker loads
        worker_loads = [0] * num_workers
        worker_files = [[] for _ in range(num_workers)]
        
        # Distribute files to balance total bytes across workers
        for file_info in sorted_files:
            # Find worker with smallest current load
            min_load_worker = min(range(num_workers), key=lambda i: worker_loads[i])
            
            # Assign file to this worker
            worker_files[min_load_worker].append(file_info)
            worker_loads[min_load_worker] += file_info.size
        
        return worker_files
    
    def get_distribution_summary(self, worker_files: List[List[FileInfo]]) -> List[dict]:
        """
        Get summary of file distribution across workers.
        
        Args:
            worker_files: List of file lists per worker
            
        Returns:
            List of dictionaries with distribution info per worker
        """
        summary = []
        
        for i, files in enumerate(worker_files):
            total_size = sum(f.size for f in files)
            avg_size = total_size / len(files) if files else 0
            
            summary.append({
                'worker_id': i + 1,
                'file_count': len(files),
                'total_size': total_size,
                'avg_file_size': avg_size
            })
        
        return summary
    
    def print_distribution_summary(self, worker_files: List[List[FileInfo]]):
        """
        Print a formatted table showing file distribution across workers.
        
        Args:
            worker_files: List of file lists per worker
        """
        from rich.console import Console
        from rich.table import Table
        
        console = Console()
        table = Table(title="Thread Distribution Summary")
        
        table.add_column("Thread", style="cyan")
        table.add_column("Files", justify="right", style="green")
        table.add_column("Total Size", justify="right", style="blue")
        table.add_column("Avg File Size", justify="right", style="magenta")
        
        summary = self.get_distribution_summary(worker_files)
        
        for info in summary:
            # Format file size
            size_str = self._format_bytes(info['total_size'])
            avg_size_str = self._format_bytes(int(info['avg_file_size']))
            
            table.add_row(
                f"Thread {info['worker_id']}",
                str(info['file_count']),
                size_str,
                avg_size_str
            )
        
        console.print(table)
    
    def _format_bytes(self, bytes_value: int) -> str:
        """
        Format bytes into human-readable string.
        
        Args:
            bytes_value: Number of bytes
            
        Returns:
            Formatted string (e.g., "1.5 MB")
        """
        if bytes_value == 0:
            return "0 bytes"
        
        units = ['bytes', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(bytes_value)
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.1f} {units[unit_index]}"


def scan_directory_for_files(directory: str, follow_symlinks: bool = False) -> Tuple[List[FileInfo], List[FileInfo]]:
    """
    Convenience function to scan a directory for files.
    
    Args:
        directory: Directory path to scan
        follow_symlinks: Whether to follow symbolic links
        
    Returns:
        Tuple of (regular_files, symlinks)
    """
    scanner = FileScanner(directory, follow_symlinks)
    return scanner.scan_directory()
