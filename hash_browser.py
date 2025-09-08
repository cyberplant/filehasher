#!/usr/bin/env python3
"""
Hash Browser - Interactive file browser for .hashes files using Rich

This script provides an interactive way to navigate through the directory
structure stored in a .hashes file, showing file sizes and total disk usage.
"""

import os
import sys
import argparse
import select
import tty
import termios
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.tree import Tree
    from rich.text import Text
    from rich.prompt import Prompt
    from rich.live import Live
    from rich.columns import Columns
    from rich.layout import Layout
    from rich.align import Align
    from rich.style import Style
    import filehasher
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please install the required dependencies: pip install rich")
    sys.exit(1)


@dataclass
class FileInfo:
    """Represents a file entry from the hash file."""
    hashkey: str
    hexdigest: str
    subdir: str
    filename: str
    file_size: int
    file_inode: int
    file_mtime: float
    full_path: str


@dataclass
class DirectoryInfo:
    """Represents directory information with aggregated data."""
    path: str
    files: List[FileInfo]
    subdirectories: List['DirectoryInfo']
    total_size: int
    file_count: int


class HashBrowser:
    """Interactive browser for .hashes files using Rich."""

    def __init__(self, hash_file: str):
        self.hash_file = hash_file
        # Force console width and disable soft wrapping; we'll crop manually
        self.console = Console(width=self.get_terminal_size()[0], soft_wrap=False)
        self.current_path = ""
        self.file_data: Dict[str, FileInfo] = {}
        self.directory_tree: Dict[str, DirectoryInfo] = {}
        self.algorithm: Optional[str] = None
        self.old_settings = None

        # ASCII-only icons (avoid wide glyphs that can cause wrapping)
        self.ICON_DIR = "[D]"
        self.ICON_FILE = "[F]"
        self.SELECT_MARK = ">"

        # Get terminal size for responsive layout
        self.term_width, self.term_height = self.get_terminal_size()

        # Load and parse the hash file
        self.load_hash_file()

    def get_terminal_size(self) -> Tuple[int, int]:
        """Get terminal width and height."""
        try:
            return shutil.get_terminal_size()
        except:
            return (80, 24)  # Fallback to reasonable defaults

    def getch(self) -> str:
        """Get a single character from stdin without waiting for Enter."""
        if os.name == 'nt':  # Windows
            import msvcrt
            return msvcrt.getch().decode('utf-8')
        else:  # Unix/Linux/Mac
            # Terminal is already in raw mode, just read one character
            return sys.stdin.read(1)

    def get_key(self) -> str:
        """Get a key press, handling special keys like arrow keys."""
        ch = self.getch()
        if ch == '\x1b':  # Escape sequence start
            # Read the next two characters for arrow keys (terminal is already in raw mode)
            seq = sys.stdin.read(2)
            if seq == '[A':  # Up arrow
                return 'up'
            elif seq == '[B':  # Down arrow
                return 'down'
            elif seq == '[C':  # Right arrow
                return 'right'
            elif seq == '[D':  # Left arrow
                return 'left'
        return ch

    def load_hash_file(self) -> None:
        """Load and parse the .hashes file."""
        if not os.path.exists(self.hash_file):
            self.console.print(f"[red]Error: Hash file '{self.hash_file}' not found.[/red]")
            sys.exit(1)

        try:
            # Use the existing filehasher function to load the hash file
            hash_dict, algorithm = filehasher._load_hashfile(self.hash_file)
            self.algorithm = algorithm

            # Convert to our FileInfo objects
            for hashkey, (hexdigest, subdir, filename, file_size, file_inode, file_mtime) in hash_dict.items():
                full_path = os.path.join(subdir, filename).replace("\\", "/")
                file_info = FileInfo(
                    hashkey=hashkey,
                    hexdigest=hexdigest,
                    subdir=subdir,
                    filename=filename,
                    file_size=file_size,
                    file_inode=file_inode,
                    file_mtime=file_mtime,
                    full_path=full_path
                )
                self.file_data[hashkey] = file_info

            # Build directory tree
            self.build_directory_tree()

            self.console.print(f"[green]Loaded {len(self.file_data)} files from {self.hash_file}[/green]")
            if self.algorithm:
                self.console.print(f"[dim]Hash algorithm: {self.algorithm}[/dim]")

            # Debug: Show directory tree keys
            if self.directory_tree:
                self.console.print(f"[dim]Directory tree has {len(self.directory_tree)} entries[/dim]")
                root_candidates = [k for k in self.directory_tree.keys() if k in ["", ".", "/"] or not k]
                if root_candidates:
                    self.console.print(f"[dim]Root candidates: {root_candidates}[/dim]")
                else:
                    # Find directory with most files
                    max_dir = max(self.directory_tree.values(), key=lambda d: d.file_count)
                    self.console.print(f"[dim]Largest directory: {max_dir.path} ({max_dir.file_count} files)[/dim]")
            else:
                self.console.print("[red]Warning: No directory tree built![/red]")

        except Exception as e:
            self.console.print(f"[red]Error loading hash file: {e}[/red]")
            sys.exit(1)

    def build_directory_tree(self) -> None:
        """Build directory tree structure from file data."""
        # Group files by directory
        dir_files = defaultdict(list)

        for file_info in self.file_data.values():
            dir_path = file_info.subdir
            if not dir_path:
                dir_path = "."
            dir_files[dir_path].append(file_info)

        # Build directory info objects
        for dir_path, files in dir_files.items():
            total_size = sum(f.file_size for f in files)
            dir_info = DirectoryInfo(
                path=dir_path,
                files=files,
                subdirectories=[],  # Will be populated below
                total_size=total_size,
                file_count=len(files)
            )
            self.directory_tree[dir_path] = dir_info

        # Build parent-child relationships
        for dir_path in list(self.directory_tree.keys()):
            parent_path = os.path.dirname(dir_path)
            if parent_path and parent_path in self.directory_tree:
                self.directory_tree[parent_path].subdirectories.append(self.directory_tree[dir_path])
            elif dir_path != "." and "." in self.directory_tree:
                self.directory_tree["."].subdirectories.append(self.directory_tree[dir_path])

    def format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        try:
            size = float(size_bytes)
        except Exception:
            return "0 B"

        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        idx = 0
        while size >= 1024.0 and idx < len(units) - 1:
            size /= 1024.0
            idx += 1
        if idx == 0:
            return f"{int(size)} {units[idx]}"
        return f"{size:.1f} {units[idx]}"

    def _crop_text(self, s: str, max_width: int) -> str:
        """Crop string to max_width with ellipsis if needed."""
        if max_width <= 3:
            return s[:max(0, max_width)]
        return s if len(s) <= max_width else (s[: max_width - 3] + "...")

    def get_current_directory_info(self) -> Optional[DirectoryInfo]:
        """Get directory info for current path."""
        # First try the current path
        if self.current_path in self.directory_tree:
            return self.directory_tree[self.current_path]

        # Try root directory variations
        for root_path in ["", ".", "/"]:
            if root_path in self.directory_tree:
                return self.directory_tree[root_path]

        # Find the directory with the most files (likely the root)
        if self.directory_tree:
            return max(self.directory_tree.values(), key=lambda d: d.file_count)

        return None

    def render_header(self) -> Panel:
        """Render the header panel with current path and stats."""
        dir_info = self.get_current_directory_info()
        if not dir_info:
            return Panel("No directory information available", title="Hash Browser", width=self.term_width)

        # Calculate total disk usage
        total_size = sum(d.total_size for d in self.directory_tree.values())
        total_files = sum(d.file_count for d in self.directory_tree.values())

        # Create ultra-compact header that fits terminal width
        available_width = self.term_width - 4  # Account for panel borders

        # Truncate path if too long
        path_display = self.current_path or "Root"
        max_path_len = available_width - 20  # Leave room for stats
        if len(path_display) > max_path_len:
            path_display = "..." + path_display[-(max_path_len - 3):]

        # Create compact single-line header
        path_part = f"📁 {path_display}"
        stats_part = f"{dir_info.file_count} files | {self.format_size(dir_info.total_size)}"
        total_part = f"Total: {total_files} files | {self.format_size(total_size)}"
        algo_part = f" | {self.algorithm}" if self.algorithm else ""

        # Calculate space allocation
        path_max = min(30, available_width // 4)
        stats_max = min(20, available_width // 4)
        total_max = min(25, available_width // 4)
        algo_max = min(10, available_width // 4)

        # Truncate parts if needed
        if len(path_part) > path_max:
            path_part = path_part[:path_max-3] + "..."
        if len(stats_part) > stats_max:
            stats_part = stats_part[:stats_max-3] + "..."
        if len(total_part) > total_max:
            total_part = total_part[:total_max-3] + "..."
        if len(algo_part) > algo_max:
            algo_part = algo_part[:algo_max-3] + "..."

        # Combine with proper spacing
        header_line = f"{path_part:<{path_max}} | {stats_part:<{stats_max}} | {total_part:<{total_max}}{algo_part}"

        # Final truncation if still too long
        if len(header_line) > available_width:
            header_line = header_line[:available_width-3] + "..."

        return Panel(header_line, title="Hash Browser", border_style="blue", width=self.term_width, padding=(0,1))

    def render_directory_listing(self) -> Table:
        """Render the current directory listing."""
        dir_info = self.get_current_directory_info()
        if not dir_info:
            table = Table(title="Directory Contents")
            table.add_column("No files found")
            return table

        table = Table(title=f"Contents of {self.current_path or 'Root'}")
        table.add_column("Type", style="dim", width=4)
        table.add_column("Name", style="cyan")
        table.add_column("Size", style="yellow", justify="right")
        table.add_column("Files", style="green", justify="right")

        # Add parent directory if not at root
        if self.current_path:
            table.add_row("📁", "..", "", "")

        # Sort directories and files
        items = []

        # Add subdirectories
        for subdir in sorted(dir_info.subdirectories, key=lambda x: x.path):
            dirname = os.path.basename(subdir.path) or subdir.path
            items.append(("📁", dirname, self.format_size(subdir.total_size), str(subdir.file_count)))

        # Add files
        for file_info in sorted(dir_info.files, key=lambda x: x.filename.lower()):
            items.append(("📄", file_info.filename, self.format_size(file_info.file_size), ""))

        # Add items to table
        for item_type, name, size, file_count in items:
            table.add_row(item_type, name, size, file_count)

        return table

    def render_directory_listing_with_selection(self, dir_info: DirectoryInfo, selected_index: int) -> None:
        """Render directory listing with visual selection indicator."""
        # Calculate responsive widths based on terminal size
        available_width = self.term_width - 4  # Leave minimal margin
        icon_width = 2
        size_width = 8
        count_width = 6
        name_width = available_width - icon_width - size_width - count_width - 4  # Account for spaces and padding

        # Ensure reasonable minimum widths
        name_width = max(name_width, 10)
        size_width = min(size_width, 8)
        count_width = min(count_width, 6)

        # If still too wide, reduce name width
        if name_width + icon_width + size_width + count_width + 4 > available_width:
            name_width = available_width - icon_width - size_width - count_width - 4
            name_width = max(name_width, 5)

        # Build list of items
        items = []

        # Add parent directory if not at root
        if self.current_path:
            items.append((self.ICON_DIR, "..", "", ""))

        # Add subdirectories
        for subdir in sorted(dir_info.subdirectories, key=lambda x: x.path):
            dirname = os.path.basename(subdir.path) or subdir.path
            # More aggressive truncation for very small terminals
            max_name_len = max(8, name_width - 3)  # Minimum 8 chars for readability
            if len(dirname) > max_name_len:
                dirname = dirname[:max_name_len-3] + "..."
            items.append((self.ICON_DIR, dirname, self.format_size(subdir.total_size), str(subdir.file_count)))

        # Add files
        for file_info in sorted(dir_info.files, key=lambda x: x.filename.lower()):
            filename = file_info.filename
            # More aggressive truncation for very small terminals
            max_name_len = max(8, name_width - 3)  # Minimum 8 chars for readability
            if len(filename) > max_name_len:
                filename = filename[:max_name_len-3] + "..."
            items.append((self.ICON_FILE, filename, self.format_size(file_info.file_size), ""))

        # Debug: Show what we have
        if not items:
            self.console.print(f"[dim]No items found in directory '{self.current_path}'[/dim]")
            self.console.print(f"[dim]Subdirs: {len(dir_info.subdirectories)}, Files: {len(dir_info.files)}[/dim]")

        # Calculate pagination - be more conservative with small terminals
        if self.term_height <= 15:
            max_items_per_page = 8  # Very small terminal
        elif self.term_height <= 20:
            max_items_per_page = 10  # Small terminal
        else:
            max_items_per_page = min(20, self.term_height - 10)  # Leave room for header and controls
        total_items = len(items)
        total_pages = (total_items + max_items_per_page - 1) // max_items_per_page if total_items > 0 else 1
        current_page = selected_index // max_items_per_page if total_items > 0 else 0
        start_idx = current_page * max_items_per_page
        end_idx = min(start_idx + max_items_per_page, total_items)

        # Display current page items with selection highlighting
        if not items:
            self.console.print("  [dim](empty directory)[/dim]")
        else:
            for i in range(start_idx, end_idx):
                item_idx = i - start_idx  # Index within current page
                item_type, name, size, file_count = items[i]

                # Create display strings that fit within terminal width
                display_name = self._crop_text(name, name_width)
                display_size = self._crop_text(size, size_width)
                display_count = self._crop_text(file_count, count_width)

                # Create the full line and ensure it fits
                line = f"{self.SELECT_MARK} {item_type} {display_name:<{name_width}} {display_size:>{size_width}} {display_count:>{count_width}}"
                if len(line) > self.term_width - 2:
                    # Truncate the name further if needed
                    available_for_name = self.term_width - len(f"{self.SELECT_MARK} {item_type}  {display_size:>{size_width}} {display_count:>{count_width}}") - 5
                    if available_for_name > 5:
                        display_name = self._crop_text(display_name, available_for_name)
                        line = f"{self.SELECT_MARK} {item_type} {display_name:<{available_for_name}} {display_size:>{size_width}} {display_count:>{count_width}}"

                if i == selected_index:
                    # Selected item - highlight it
                    self.console.print(self._crop_text(f"[bold white on blue]{line}[/bold white on blue]", self.term_width))
                else:
                    # Regular item
                    normal_line = f"  {item_type} {display_name:<{name_width}} {display_size:>{size_width}} {display_count:>{count_width}}"
                    self.console.print(self._crop_text(normal_line, self.term_width))

            # Show pagination info if needed
            if total_pages > 1:
                page_info = f"[dim]Page {current_page + 1}/{total_pages} ({start_idx + 1}-{end_idx} of {total_items})[/dim]"
                # Ensure pagination info fits
                if len(page_info) > self.term_width - 4:
                    page_info = f"[dim]Pg {current_page + 1}/{total_pages} ({start_idx + 1}-{end_idx})[/dim]"
                self.console.print(page_info)

    def handle_selection(self, selected_index: int) -> bool:
        """Handle selection of an item. Returns True if directory changed."""
        dir_info = self.get_current_directory_info()
        if not dir_info:
            return False

        items = []

        # Add parent directory if not at root
        if self.current_path:
            items.append(("..", None, "directory"))

        # Add subdirectories
        for subdir in sorted(dir_info.subdirectories, key=lambda x: x.path):
            dirname = os.path.basename(subdir.path) or subdir.path
            items.append((dirname, subdir, "directory"))

        # Add files
        for file_info in sorted(dir_info.files, key=lambda x: x.filename.lower()):
            items.append((file_info.filename, file_info, "file"))

        # Calculate pagination to get the actual item index
        max_items_per_page = self.term_height - 12
        total_items = len(items)
        total_pages = (total_items + max_items_per_page - 1) // max_items_per_page if total_items > 0 else 1
        current_page = selected_index // max_items_per_page
        start_idx = current_page * max_items_per_page

        # Adjust selected_index to account for pagination
        actual_index = start_idx + (selected_index % max_items_per_page)

        if actual_index >= len(items):
            return False

        name, item, item_type = items[actual_index]

        if item_type == "directory":
            if name == ".." and self.current_path:
                self.current_path = os.path.dirname(self.current_path)
                return True
            elif isinstance(item, DirectoryInfo):
                self.current_path = item.path
                return True
        elif item_type == "file" and isinstance(item, FileInfo):
            # Show file details
            self.console.clear()
            file_panel = self.render_file_details(item)
            self.console.print(file_panel)
            self.console.print("\n[dim]Press any key to continue...[/dim]")
            self.console.input()
            return False

        return False

    def render_file_details(self, file_info: FileInfo) -> Panel:
        """Render detailed information about a selected file."""
        # Truncate long paths and filenames to fit terminal
        max_detail_width = self.term_width - 8  # Account for panel borders and padding

        filename_display = file_info.filename
        if len(filename_display) > max_detail_width - 10:
            filename_display = filename_display[:max_detail_width - 13] + "..."

        path_display = file_info.full_path
        if len(path_display) > max_detail_width - 6:
            path_display = "..." + path_display[-(max_detail_width - 9):]

        hash_display = file_info.hexdigest
        if len(hash_display) > max_detail_width - 6:
            hash_display = hash_display[:max_detail_width - 9] + "..."

        details = Text()
        details.append(f"Name: {filename_display}\n", style="bold")
        details.append(f"Path: {path_display}\n", style="cyan")
        details.append(f"Size: {self.format_size(file_info.file_size)}\n", style="yellow")
        details.append(f"Hash: {hash_display}\n", style="green")
        if self.algorithm:
            details.append(f"Alg: {self.algorithm}\n", style="blue")
        details.append(f"Inode: {file_info.file_inode}", style="dim")

        return Panel(details, title="📄 File Details", border_style="green", width=self.term_width)

    def navigate_to(self, path: str) -> bool:
        """Navigate to a new directory path."""
        if path == ".." and self.current_path:
            self.current_path = os.path.dirname(self.current_path)
            return True

        dir_info = self.get_current_directory_info()
        if dir_info and path in [d.path for d in dir_info.subdirectories]:
            self.current_path = path
            return True
        return False

    def run_interactive(self) -> None:
        """Run the interactive browser."""
        selected_index = 0
        old_settings = None

        # Save terminal settings and set raw mode
        if os.name != 'nt':  # Unix/Linux/Mac
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            tty.setraw(fd)

        try:
            while True:
                # Update terminal size in case window was resized
                new_width, new_height = self.get_terminal_size()
                if new_width != self.term_width or new_height != self.term_height:
                    self.term_width, self.term_height = new_width, new_height
                    self.console = Console(width=self.term_width)

                # Clear screen and render current state
                self.console.clear()
                import time
                time.sleep(0.05)  # Small delay to prevent rapid screen updates

                # Render header directly (avoid Panel object issues)
                dir_info = self.get_current_directory_info()
                if dir_info:
                    # Create ultra-compact header that fits terminal width
                    available_width = self.term_width - 4  # Account for panel borders

                    # Truncate path if too long
                    path_display = self.current_path or "Root"
                    max_path_len = min(25, available_width // 3)
                    if len(path_display) > max_path_len:
                        path_display = "..." + path_display[-(max_path_len - 3):]

                    # Single line format
                    header_line = f"[D] {path_display} | {dir_info.file_count} files | {self.format_size(dir_info.total_size)}"
                    if self.algorithm:
                        header_line += f" | {self.algorithm}"

                    # Ensure it fits
                    if len(header_line) > available_width:
                        header_line = header_line[:available_width-3] + "..."

                    # Print header with styling
                    self.console.print(self._crop_text(f"[bold blue]{header_line}[/bold blue]", self.term_width))
                else:
                    self.console.print("[red]No directory information available[/red]")
                self.console.print()

                # Render directory listing with selection
                dir_info = self.get_current_directory_info()
                if dir_info:
                    self.render_directory_listing_with_selection(dir_info, selected_index)
                else:
                    self.console.print("[red]No directory information available[/red]")

                # Show compact controls
                self.console.print()
                controls_text = "↑↓/jk: Navigate | Enter: Select | Bksp: Up | q: Quit | s: Search"
                self.console.print(f"[dim]{controls_text}[/dim]")

                # Get user input
                try:
                    key = self.get_key()

                    if key.lower() == 'q':
                        break
                    elif key in ['up', 'k']:  # Up arrow or k
                        selected_index = max(0, selected_index - 1)
                    elif key in ['down', 'j']:  # Down arrow or j
                        dir_info = self.get_current_directory_info()
                        if dir_info:
                            max_items = len(dir_info.subdirectories) + len(dir_info.files)
                            if self.current_path:  # Account for ".." entry
                                max_items += 1
                            selected_index = min(max_items - 1, selected_index + 1)
                    elif key in ['\x1b[5~', 'pgup']:  # Page Up
                        # Go to previous page
                        dir_info = self.get_current_directory_info()
                        if dir_info:
                            max_items_per_page = self.term_height - 12
                            current_page = selected_index // max_items_per_page
                            if current_page > 0:
                                selected_index = (current_page - 1) * max_items_per_page
                    elif key in ['\x1b[6~', 'pgdown']:  # Page Down
                        # Go to next page
                        dir_info = self.get_current_directory_info()
                        if dir_info:
                            max_items = len(dir_info.subdirectories) + len(dir_info.files)
                            if self.current_path:
                                max_items += 1
                            max_items_per_page = self.term_height - 12
                            current_page = selected_index // max_items_per_page
                            total_pages = (max_items + max_items_per_page - 1) // max_items_per_page
                            if current_page < total_pages - 1:
                                selected_index = min(max_items - 1, (current_page + 1) * max_items_per_page)
                    elif key in ['\n', '\r']:  # Enter
                        if self.handle_selection(selected_index):
                            selected_index = 0  # Reset selection when changing directories
                    elif key == '\x7f' or key == '\x08':  # Backspace
                        if self.current_path:
                            self.current_path = os.path.dirname(self.current_path)
                            selected_index = 0
                    elif key.lower() == 's':
                        # Search functionality - temporarily restore normal mode
                        if os.name != 'nt':
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

                        self.console.clear()
                        search_term = Prompt.ask("Search for files")
                        if search_term:
                            self.search_files(search_term)
                            self.console.print("\n[dim]Press any key to continue...[/dim]")
                            input()  # Use regular input for the pause

                        # Restore raw mode
                        if os.name != 'nt':
                            tty.setraw(fd)

                except KeyboardInterrupt:
                    break
                except EOFError:
                    break

        finally:
            # Restore terminal settings
            if os.name != 'nt' and old_settings:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    def search_files(self, search_term: str) -> None:
        """Search for files matching the search term."""
        results = []
        for file_info in self.file_data.values():
            if search_term.lower() in file_info.filename.lower() or search_term.lower() in file_info.full_path.lower():
                results.append(file_info)

        if not results:
            self.console.print(f"[yellow]No files found matching: {search_term}[/yellow]")
            return

        table = Table(title=f"Search Results for '{search_term}'")
        table.add_column("Path", style="cyan")
        table.add_column("Size", style="yellow", justify="right")

        for file_info in sorted(results, key=lambda x: x.filename.lower()):
            table.add_column(file_info.full_path, self.format_size(file_info.file_size))

        self.console.print(table)


def main():
    parser = argparse.ArgumentParser(
        description='Interactive browser for .hashes files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                         # Browse .hashes in current directory
  %(prog)s myhashes.sha256         # Browse specific hash file
  %(prog)s --help                  # Show this help

Navigation:
  ↑↓ arrows: Navigate through files/directories
  Enter: Select directory or view file details
  s: Search for files
  q: Quit
        """
    )

    parser.add_argument('hashfile', default='.hashes', nargs='?',
                        help="Hash file to browse (default: .hashes)")

    args = parser.parse_args()

    # Create and run the browser
    browser = HashBrowser(args.hashfile)
    browser.run_interactive()


if __name__ == "__main__":
    main()
