#!/usr/bin/env python3
"""
Hashfile Browser - A simple ncdu-inspired browser for .hashes files
"""

import os
import sys
import termios
import tty
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# Import from filehasher to load hash files
try:
    from filehasher import _load_hashfile
except ImportError:
    print("Error: Could not import filehasher. Make sure it's installed.")
    sys.exit(1)


@dataclass
class FileEntry:
    """Represents a file with its hash information."""
    path: str
    size: int
    hash_value: str


@dataclass
class DirEntry:
    """Represents a directory with its contents and total size."""
    name: str
    path: str
    size: int
    file_count: int
    files: List[FileEntry]
    subdirs: List['DirEntry']
    parent: Optional['DirEntry'] = None


class HashfileBrowser:
    """Simple ncdu-inspired browser for hashfiles."""
    
    def __init__(self):
        self.root_dir: Optional[DirEntry] = None
        self.current_dir: Optional[DirEntry] = None
        self.selected_index = 0
        self.scroll_offset = 0
        
    def load_hashfile(self, filepath: str) -> bool:
        """Load and parse a .hashes file."""
        try:
            print(f"Loading {filepath}...")
            hash_dict, algorithm = _load_hashfile(filepath)
            print(f"Loaded {len(hash_dict)} files using {algorithm} algorithm")
            
            # Convert dict to list of tuples (filepath, size, hash)
            files_data = []
            for hashkey, file_info in hash_dict.items():
                # file_info is (hexdigest, subdir, filename, file_size, file_inode, file_mtime)
                hexdigest, subdir, filename, file_size, file_inode, file_mtime = file_info
                
                # Reconstruct full path using forward slashes (platform independent)
                if subdir:
                    # Normalize path separators
                    subdir_normalized = subdir.replace('\\', '/')
                    full_path = subdir_normalized + '/' + filename
                else:
                    full_path = filename
                
                # Make paths relative by finding a common meaningful root
                # Strip common OS-specific prefixes to make truly portable
                if full_path.startswith('/Users/'):
                    # macOS paths - use everything after /Users/username/
                    parts = full_path.split('/')
                    if len(parts) > 3:  # ['', 'Users', 'username', ...]
                        full_path = '/'.join(parts[3:])  # Start from after username
                elif full_path.startswith('/home/'):
                    # Linux paths - use everything after /home/username/
                    parts = full_path.split('/')
                    if len(parts) > 3:  # ['', 'home', 'username', ...]
                        full_path = '/'.join(parts[3:])  # Start from after username
                elif full_path.startswith('C:\\Users\\') or full_path.startswith('C:/Users/'):
                    # Windows paths - use everything after C:/Users/username/
                    parts = full_path.replace('\\', '/').split('/')
                    if len(parts) > 3:  # ['C:', 'Users', 'username', ...]
                        full_path = '/'.join(parts[3:])  # Start from after username
                elif full_path.startswith('/'):
                    # Other absolute paths - just strip the leading slash
                    full_path = full_path.lstrip('/')
                
                files_data.append((full_path, int(file_size), hexdigest))
            
            # Build directory tree
            self._build_tree(files_data)
            print(f"Built tree from {len(files_data)} files")
            return True
            
        except Exception as e:
            print(f"Error loading hashfile: {e}")
            return False
    
    def _build_tree(self, files_data: List[Tuple[str, int, str]]) -> None:
        """Build directory tree from file data based purely on the paths in the hash file."""
        # Create root directory
        self.root_dir = DirEntry("(Hash File Root)", "", 0, 0, [], [])
        self.current_dir = self.root_dir
        
        # Dictionary to store all directories by their path
        dirs: Dict[str, DirEntry] = {"": self.root_dir}
        
        # Process each file and build the full directory structure
        for filepath, size, hash_value in files_data:
            # Normalize path separators to forward slashes
            normalized_path = filepath.replace('\\', '/')
            
            # Create file entry
            file_entry = FileEntry(normalized_path, size, hash_value)
            
            # Get directory path (everything except the filename)
            if '/' in normalized_path:
                dir_path = '/'.join(normalized_path.split('/')[:-1])
                filename = normalized_path.split('/')[-1]
            else:
                # File in root
                dir_path = ""
                filename = normalized_path
            
            # Ensure the directory exists
            self._ensure_dir_exists(dirs, dir_path)
            
            # Add file to its directory
            dirs[dir_path].files.append(file_entry)
        
        # Build parent-child relationships
        self._link_directories(dirs)
        
        # Calculate directory sizes (sum of files + subdirectories)
        self._calculate_totals(self.root_dir)
    
    def _ensure_dir_exists(self, dirs: Dict[str, DirEntry], dir_path: str) -> None:
        """Ensure a directory path exists in the tree."""
        if dir_path in dirs:
            return
            
        # Split path into components
        if dir_path:
            path_parts = dir_path.split('/')
        else:
            return  # Root already exists
            
        # Build path progressively
        current_path = ""
        for i, part in enumerate(path_parts):
            if i == 0:
                current_path = part
            else:
                current_path = current_path + "/" + part
                
            if current_path not in dirs:
                # Create directory entry
                dir_name = part
                new_dir = DirEntry(dir_name, current_path, 0, 0, [], [])
                dirs[current_path] = new_dir
    
    def _link_directories(self, dirs: Dict[str, DirEntry]) -> None:
        """Link child directories to their parents."""
        for path, dir_entry in dirs.items():
            if path == "":  # Skip root
                continue
                
            # Find parent path
            if '/' in path:
                parent_path = '/'.join(path.split('/')[:-1])
            else:
                parent_path = ""
                
            # Link to parent
            if parent_path in dirs:
                parent = dirs[parent_path]
                dir_entry.parent = parent
                if dir_entry not in parent.subdirs:
                    parent.subdirs.append(dir_entry)
    
    
    def _calculate_totals(self, dir_entry: DirEntry) -> None:
        """Calculate total sizes including subdirectories."""
        total_size = sum(f.size for f in dir_entry.files)
        total_files = len(dir_entry.files)
        
        for subdir in dir_entry.subdirs:
            self._calculate_totals(subdir)
            total_size += subdir.size
            total_files += subdir.file_count
        
        dir_entry.size = total_size
        dir_entry.file_count = total_files
    
    def format_size(self, size: int) -> str:
        """Format size in human readable format."""
        units = ["B", "KB", "MB", "GB", "TB"]
        size_f = float(size)
        unit_idx = 0
        
        while size_f >= 1024 and unit_idx < len(units) - 1:
            size_f /= 1024
            unit_idx += 1
        
        if unit_idx == 0:
            return f"{int(size_f)} {units[unit_idx]}"
        else:
            return f"{size_f:.1f} {units[unit_idx]}"
    
    def get_terminal_size(self) -> Tuple[int, int]:
        """Get terminal dimensions."""
        try:
            return os.get_terminal_size()
        except:
            return (80, 24)  # Default fallback
    
    def render_screen(self) -> None:
        """Render the current screen."""
        width, height = self.get_terminal_size()
        
        # Clear screen
        print("\033[2J\033[H", end="")
        
        # Header
        path_str = self.current_dir.path or "/"
        header = f" {path_str} - {self.format_size(self.current_dir.size)} ({self.current_dir.file_count} files)"
        print(header[:width])
        print("-" * min(len(header), width))
        
        # Get items to display
        items = []
        
        # Add parent directory if not at root
        if self.current_dir.parent:
            items.append(("../", 0, True, None))
        
        # Add subdirectories
        for subdir in sorted(self.current_dir.subdirs, key=lambda x: x.name.lower()):
            items.append((f"{subdir.name}/", subdir.size, True, subdir))
        
        # Add files
        for file_entry in sorted(self.current_dir.files, key=lambda x: x.path.lower()):
            filename = os.path.basename(file_entry.path)
            items.append((filename, file_entry.size, False, file_entry))
        
        # Calculate display area
        display_height = height - 4  # Header + separator + footer
        
        # Adjust scroll if needed
        if self.selected_index < self.scroll_offset:
            self.scroll_offset = self.selected_index
        elif self.selected_index >= self.scroll_offset + display_height:
            self.scroll_offset = self.selected_index - display_height + 1
        
        # Display items
        for i in range(display_height):
            item_index = self.scroll_offset + i
            if item_index >= len(items):
                print()
                continue
            
            name, size, is_dir, obj = items[item_index]
            size_str = self.format_size(size)
            
            # Create line with proper spacing
            marker = ">" if item_index == self.selected_index else " "
            line = f"{marker} {name:<40} {size_str:>10}"
            
            # Truncate to terminal width
            print(line[:width])
        
        # Footer
        print("-" * min(60, width))
        print("Use arrows/hjkl/Ctrl+B/F/Space/b to navigate, Enter/l to select, q to quit")
    
    def get_key(self) -> str:
        """Get a single keypress."""
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            key = sys.stdin.read(1)

            # Handle escape sequences
            if key == '\033':
                # Read more characters to get the full sequence
                key += sys.stdin.read(1)
                if key[-1] in '[{':
                    key += sys.stdin.read(1)
                    if key[-1] in '~0123456789':
                        key += sys.stdin.read(1)

        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

        return key
    
    def navigate(self, direction: int) -> None:
        """Navigate up/down in current directory."""
        items_count = len(self.get_current_items())
        if items_count == 0:
            return

        self.selected_index += direction
        self.selected_index = max(0, min(self.selected_index, items_count - 1))

    def navigate_page(self, direction: int) -> None:
        """Navigate by page in current directory."""
        width, height = self.get_terminal_size()
        display_height = height - 4  # Header + separator + footer

        items_count = len(self.get_current_items())
        if items_count == 0:
            return

        if direction > 0:  # Page down
            self.selected_index += display_height
        else:  # Page up
            self.selected_index -= display_height

        self.selected_index = max(0, min(self.selected_index, items_count - 1))
    
    def get_current_items(self) -> List:
        """Get list of current directory items."""
        items = []

        if self.current_dir.parent:
            items.append(("../", 0, True, None))

        # Sort subdirectories by name (same as in render_screen)
        for subdir in sorted(self.current_dir.subdirs, key=lambda x: x.name.lower()):
            items.append((f"{subdir.name}/", subdir.size, True, subdir))

        # Sort files by name (same as in render_screen)
        for file_entry in sorted(self.current_dir.files, key=lambda x: x.path.lower()):
            filename = os.path.basename(file_entry.path)
            items.append((filename, file_entry.size, False, file_entry))

        return items
    
    def enter_selected(self) -> None:
        """Enter the selected directory or file."""
        items = self.get_current_items()
        if not items or self.selected_index >= len(items):
            return
        
        name, size, is_dir, obj = items[self.selected_index]
        
        if is_dir:
            if obj is None:  # Parent directory
                if self.current_dir.parent:
                    self.current_dir = self.current_dir.parent
                    self.selected_index = 0
                    self.scroll_offset = 0
            else:  # Subdirectory
                self.current_dir = obj
                self.selected_index = 0
                self.scroll_offset = 0
    
    def run(self) -> None:
        """Main application loop."""
        if not self.root_dir:
            print("No hashfile loaded!")
            return
        
        try:
            while True:
                self.render_screen()

                key = self.get_key()

                if key == 'q':
                    break
                # Arrow keys and vim navigation
                elif key in ['\033[A', 'k']:  # Up arrow or vim k
                    self.navigate(-1)
                elif key in ['\033[B', 'j']:  # Down arrow or vim j
                    self.navigate(1)
                elif key in ['\033[D', 'h']:  # Left arrow or vim h (go to parent)
                    if self.current_dir.parent:
                        self.current_dir = self.current_dir.parent
                        self.selected_index = 0
                        self.scroll_offset = 0
                elif key in ['\033[C', 'l']:  # Right arrow or vim l (enter directory)
                    self.enter_selected()
                # Page navigation - multiple sequences for cross-platform compatibility
                elif key in ['\033[5~', '\033[[5~', '\002', '\x02']:  # Page Up (Ctrl+B)
                    self.navigate_page(-1)
                elif key in ['\033[6~', '\033[[6~', '\006', '\x06']:  # Page Down (Ctrl+F)
                    self.navigate_page(1)
                # Alternative page navigation
                elif key == ' ':  # Space - Page Down
                    self.navigate_page(1)
                elif key == 'b':  # 'b' - Page Up
                    self.navigate_page(-1)
                elif key == '\r' or key == '\n':  # Enter
                    self.enter_selected()
                    
        except KeyboardInterrupt:
            pass
        finally:
            print("\033[2J\033[H")  # Clear screen
            print("Goodbye!")


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python hashfile_browser.py <hashfile>")
        sys.exit(1)
    
    hashfile = sys.argv[1]
    if not os.path.exists(hashfile):
        print(f"Error: File '{hashfile}' not found")
        sys.exit(1)
    
    browser = HashfileBrowser()
    if browser.load_hashfile(hashfile):
        browser.run()


if __name__ == "__main__":
    main()
