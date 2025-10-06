"""
File scanner for efficient directory traversal and file discovery
"""

import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Generator
from dataclasses import dataclass
import logging
import random

logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """Information about a file to be hashed"""
    path: Path
    relative_path: Path
    size: int
    inode: int
    mtime: float
    is_symlink: bool = False
    
    def __post_init__(self):
        """Validate file info after initialization"""
        if not self.path.exists():
            raise FileNotFoundError(f"File does not exist: {self.path}")
    
    @property
    def directory(self) -> str:
        """Get directory part of relative path as string"""
        if self.relative_path.parent == Path('.'):
            return '.'
        return str(self.relative_path.parent)
    
    @property
    def filename(self) -> str:
        """Get filename part as string"""
        return self.relative_path.name
    
    def to_hash_entry(self, primary_hash: str, secondary_hash: str) -> str:
        """Convert to hash file entry format"""
        if self.is_symlink:
            return f"# SYMLINK|{self.directory}|{self.filename}|{self.size}|{self.inode}|{int(self.mtime)}"
        else:
            return f"{primary_hash}|{secondary_hash}|{self.directory}|{self.filename}|{self.size}|{self.inode}|{int(self.mtime)}"


class FileScanner:
    """Scans directories for files and provides load balancing information"""
    
    def __init__(self, follow_symlinks: bool = False):
        self.follow_symlinks = follow_symlinks
        self._scanned_files: List[FileInfo] = []
        self._total_size = 0
        self._scan_time = 0.0
    
    def scan_directory(self, directory: Path, exclude_patterns: Optional[List[str]] = None) -> List[FileInfo]:
        """
        Scan directory for files
        
        Args:
            directory: Directory to scan
            exclude_patterns: List of glob patterns to exclude
        
        Returns:
            List of FileInfo objects
        """
        start_time = time.time()
        self._scanned_files.clear()
        self._total_size = 0
        
        logger.info(f"Scanning directory: {directory}")
        
        try:
            for file_info in self._scan_recursive(directory, directory, exclude_patterns):
                self._scanned_files.append(file_info)
                self._total_size += file_info.size
                
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")
            raise
        
        self._scan_time = time.time() - start_time
        logger.info(f"Scanned {len(self._scanned_files)} files, {self._total_size:,} bytes in {self._scan_time:.2f}s")
        
        return self._scanned_files.copy()
    
    def _scan_recursive(self, root_dir: Path, current_dir: Path, 
                       exclude_patterns: Optional[List[str]] = None) -> Generator[FileInfo, None, None]:
        """Recursively scan directory for files"""
        
        try:
            for item in current_dir.iterdir():
                # Skip hidden files and directories (starting with .)
                if item.name.startswith('.'):
                    continue
                
                # Check exclude patterns
                if exclude_patterns and self._should_exclude(item, exclude_patterns):
                    continue
                
                try:
                    if item.is_file():
                        # Regular file
                        stat = item.stat()
                        relative_path = item.relative_to(root_dir)
                        
                        yield FileInfo(
                            path=item,
                            relative_path=relative_path,
                            size=stat.st_size,
                            inode=stat.st_ino,
                            mtime=stat.st_mtime
                        )
                    
                    elif item.is_dir():
                        # Recursively scan subdirectories
                        yield from self._scan_recursive(root_dir, item, exclude_patterns)
                    
                    elif item.is_symlink():
                        # Handle symlinks
                        relative_path = item.relative_to(root_dir)
                        
                        if self.follow_symlinks:
                            # Follow symlink and treat as regular file/dir
                            try:
                                target = item.resolve()
                                if target.is_file():
                                    stat = target.stat()
                                    yield FileInfo(
                                        path=item,
                                        relative_path=relative_path,
                                        size=stat.st_size,
                                        inode=stat.st_ino,
                                        mtime=stat.st_mtime
                                    )
                                elif target.is_dir():
                                    # Recursively scan symlinked directory
                                    yield from self._scan_recursive(root_dir, target, exclude_patterns)
                            except (OSError, FileNotFoundError):
                                # Symlink target doesn't exist, record as symlink
                                stat = item.stat()
                                yield FileInfo(
                                    path=item,
                                    relative_path=relative_path,
                                    size=0,
                                    inode=stat.st_ino,
                                    mtime=stat.st_mtime,
                                    is_symlink=True
                                )
                        else:
                            # Don't follow symlinks, record as symlink
                            stat = item.stat()
                            yield FileInfo(
                                path=item,
                                relative_path=relative_path,
                                size=0,
                                inode=stat.st_ino,
                                mtime=stat.st_mtime,
                                is_symlink=True
                            )
                
                except (OSError, PermissionError) as e:
                    logger.warning(f"Permission denied or error accessing {item}: {e}")
                    continue
        
        except PermissionError as e:
            logger.warning(f"Permission denied accessing directory {current_dir}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error scanning {current_dir}: {e}")
    
    def _should_exclude(self, path: Path, exclude_patterns: List[str]) -> bool:
        """Check if path should be excluded based on patterns"""
        path_str = str(path)
        for pattern in exclude_patterns:
            if path.match(pattern) or pattern in path_str:
                return True
        return False
    
    def distribute_files_for_processing(self, num_workers: int) -> List[List[FileInfo]]:
        """
        Distribute files across workers based on total bytes, not file count
        
        Args:
            num_workers: Number of worker processes
        
        Returns:
            List of file lists, one per worker
        """
        if not self._scanned_files:
            return [[] for _ in range(num_workers)]
        
        # Sort files by size (largest first) to distribute load better
        sorted_files = sorted(self._scanned_files, key=lambda f: f.size, reverse=True)
        
        # Initialize worker loads (total bytes per worker)
        worker_loads = [0] * num_workers
        worker_files = [[] for _ in range(num_workers)]
        
        # Distribute files to worker with least load
        for file_info in sorted_files:
            # Find worker with minimum load
            min_worker = min(range(num_workers), key=lambda w: worker_loads[w])
            
            # Assign file to this worker
            worker_files[min_worker].append(file_info)
            worker_loads[min_worker] += file_info.size
        
        for id_worker in range(num_workers):
            random.shuffle(worker_files[id_worker])

        # Log distribution summary
        logger.info("File distribution across workers:")
        for i, (files, load) in enumerate(zip(worker_files, worker_loads)):
            if files:
                avg_size = load / len(files) if files else 0
                logger.info(f"  Worker {i+1}: {len(files)} files, {load:,} bytes, avg {avg_size:.0f} bytes/file")
        
        return worker_files
    
    def get_statistics(self) -> Dict:
        """Get scanning statistics"""
        if not self._scanned_files:
            return {
                'total_files': 0,
                'total_size': 0,
                'scan_time': 0,
                'files_per_second': 0,
                'bytes_per_second': 0
            }
        
        files_per_second = len(self._scanned_files) / self._scan_time if self._scan_time > 0 else 0
        bytes_per_second = self._total_size / self._scan_time if self._scan_time > 0 else 0
        
        return {
            'total_files': len(self._scanned_files),
            'total_size': self._total_size,
            'scan_time': self._scan_time,
            'files_per_second': files_per_second,
            'bytes_per_second': bytes_per_second
        }
    
    def scan_directory_for_update(self, directory: Path, existing_hashes: Dict[str, Tuple[int, float]], 
                                 exclude_patterns: Optional[List[str]] = None) -> List[FileInfo]:
        """
        Scan directory and only return files that need updating based on modification time
        
        Args:
            directory: Directory to scan
            existing_hashes: Dict mapping relative_path to (size, mtime)
            exclude_patterns: List of glob patterns to exclude
        
        Returns:
            List of FileInfo objects that need updating
        """
        start_time = time.time()
        self._scanned_files.clear()
        self._total_size = 0
        
        logger.info(f"Scanning directory for updates: {directory}")
        
        try:
            for file_info in self._scan_recursive_for_update(directory, directory, existing_hashes, exclude_patterns):
                self._scanned_files.append(file_info)
                self._total_size += file_info.size
                
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")
            raise
        
        self._scan_time = time.time() - start_time
        logger.info(f"Found {len(self._scanned_files)} files to update in {self._scan_time:.2f}s")
        
        return self._scanned_files.copy()
    
    def _scan_recursive_for_update(self, root_dir: Path, current_dir: Path, 
                                  existing_hashes: Dict[str, Tuple[int, float]],
                                  exclude_patterns: Optional[List[str]] = None) -> Generator[FileInfo, None, None]:
        """Recursively scan directory for files that need updating"""
        
        try:
            for item in current_dir.iterdir():
                # Skip hidden files and directories (starting with .)
                if item.name.startswith('.'):
                    continue
                
                # Check exclude patterns
                if exclude_patterns and self._should_exclude(item, exclude_patterns):
                    continue
                
                try:
                    if item.is_file():
                        # Regular file - check if it needs updating
                        relative_path = item.relative_to(root_dir)
                        relative_path_str = str(relative_path)
                        
                        # Check if file has changed
                        needs_update = True
                        if relative_path_str in existing_hashes:
                            existing_size, existing_mtime = existing_hashes[relative_path_str]
                            stat = item.stat()
                            
                            # Check if file has changed (1 second tolerance for mtime)
                            if (stat.st_size == existing_size and 
                                abs(stat.st_mtime - existing_mtime) <= 1.0):
                                needs_update = False
                        
                        if needs_update:
                            stat = item.stat()
                            yield FileInfo(
                                path=item,
                                relative_path=relative_path,
                                size=stat.st_size,
                                inode=stat.st_ino,
                                mtime=stat.st_mtime
                            )
                    
                    elif item.is_dir():
                        # Recursively scan subdirectories
                        yield from self._scan_recursive_for_update(root_dir, item, existing_hashes, exclude_patterns)
                    
                    elif item.is_symlink():
                        # Handle symlinks - always include for recording
                        relative_path = item.relative_to(root_dir)
                        stat = item.stat()
                        
                        if self.follow_symlinks:
                            try:
                                target = item.resolve()
                                if target.is_file():
                                    stat = target.stat()
                                    yield FileInfo(
                                        path=item,
                                        relative_path=relative_path,
                                        size=stat.st_size,
                                        inode=stat.st_ino,
                                        mtime=stat.st_mtime
                                    )
                                elif target.is_dir():
                                    yield from self._scan_recursive_for_update(root_dir, target, existing_hashes, exclude_patterns)
                            except (OSError, FileNotFoundError):
                                # Symlink target doesn't exist, record as symlink
                                yield FileInfo(
                                    path=item,
                                    relative_path=relative_path,
                                    size=0,
                                    inode=stat.st_ino,
                                    mtime=stat.st_mtime,
                                    is_symlink=True
                                )
                        else:
                            # Don't follow symlinks, record as symlink
                            yield FileInfo(
                                path=item,
                                relative_path=relative_path,
                                size=0,
                                inode=stat.st_ino,
                                mtime=stat.st_mtime,
                                is_symlink=True
                            )
                
                except (OSError, PermissionError) as e:
                    logger.warning(f"Permission denied or error accessing {item}: {e}")
                    continue
        
        except PermissionError as e:
            logger.warning(f"Permission denied accessing directory {current_dir}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error scanning {current_dir}: {e}")
    
def get_directory_info(directory: Path) -> Dict:
    """Get basic information about a directory"""
    try:
        stat = directory.stat()
        return {
            'path': str(directory),
            'exists': directory.exists(),
            'is_directory': directory.is_dir(),
            'readable': os.access(directory, os.R_OK),
            'size': stat.st_size if directory.is_file() else None,
            'mtime': stat.st_mtime,
            'inode': stat.st_ino
        }
    except Exception as e:
        return {
            'path': str(directory),
            'exists': False,
            'error': str(e)
        }
