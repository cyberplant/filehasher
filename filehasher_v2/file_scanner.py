"""
File scanning and discovery functionality
"""

import os
import random
from pathlib import Path
from typing import List, Tuple, NamedTuple
from dataclasses import dataclass


@dataclass
class FileInfo:
    """Information about a file to be processed"""
    path: Path
    relative_path: Path
    size: int
    inode: int
    mtime: int
    is_symlink: bool


class FileScanner:
    """Scans directories and collects file information"""
    
    def __init__(self, base_dir: Path, follow_symlinks: bool = False):
        self.base_dir = base_dir.resolve()
        self.follow_symlinks = follow_symlinks
    
    def scan_directory(self, directory: Path = None) -> List[FileInfo]:
        """Scan directory and return list of FileInfo objects"""
        if directory is None:
            directory = self.base_dir
        
        files = []
        
        try:
            for root, dirs, filenames in os.walk(directory, followlinks=self.follow_symlinks):
                root_path = Path(root)
                
                for filename in filenames:
                    file_path = root_path / filename
                    
                    try:
                        # Get file info
                        stat = file_path.stat()
                        relative_path = file_path.relative_to(self.base_dir)
                        
                        file_info = FileInfo(
                            path=file_path,
                            relative_path=relative_path,
                            size=stat.st_size,
                            inode=stat.st_ino,
                            mtime=int(stat.st_mtime),
                            is_symlink=file_path.is_symlink()
                        )
                        
                        files.append(file_info)
                        
                    except (OSError, IOError) as e:
                        # Skip files we can't access
                        print(f"Warning: Cannot access {file_path}: {e}")
                        continue
                        
        except (OSError, IOError) as e:
            raise ScanError(f"Failed to scan directory {directory}: {e}")
        
        return files
    
    def load_balance_files(self, files: List[FileInfo], num_processes: int) -> List[List[FileInfo]]:
        """
        Distribute files across processes based on total bytes, not file count.
        Randomizes order to prevent all large files being processed simultaneously.
        """
        if num_processes <= 1:
            return [files]
        
        # Sort files by size (largest first)
        sorted_files = sorted(files, key=lambda f: f.size, reverse=True)
        
        # Create buckets with total byte counts
        buckets = [[] for _ in range(num_processes)]
        bucket_sizes = [0] * num_processes
        
        # Distribute files to balance total bytes
        for file_info in sorted_files:
            # Find bucket with smallest total size
            smallest_bucket = min(range(num_processes), key=lambda i: bucket_sizes[i])
            buckets[smallest_bucket].append(file_info)
            bucket_sizes[smallest_bucket] += file_info.size
        
        # Randomize order within each bucket to prevent clustering
        for bucket in buckets:
            random.shuffle(bucket)
        
        return buckets


class ScanError(Exception):
    """Exception raised for file scanning errors"""
    pass


def get_system_info() -> dict:
    """Get system information for metadata headers"""
    import getpass
    import socket
    
    return {
        'hostname': socket.gethostname(),
        'username': getpass.getuser(),
    }
