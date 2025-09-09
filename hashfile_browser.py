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

# ANSI Color codes
class Colors:
    # Reset
    RESET = '\033[0m'

    # Regular colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'

    # Bright colors
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'

    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'

    # Styles
    BOLD = '\033[1m'
    DIM = '\033[2m'
    REVERSE = '\033[7m'


@dataclass
class FileEntry:
    """Represents a file with its hash information."""
    path: str
    size: int
    hash_value: str

    def __hash__(self):
        return hash((self.path, self.size, self.hash_value))

    def __eq__(self, other):
        if not isinstance(other, FileEntry):
            return False
        return (self.path, self.size, self.hash_value) == (other.path, other.size, other.hash_value)


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

    def __hash__(self):
        return hash((self.path, self.size, self.file_count))

    def __eq__(self, other):
        if not isinstance(other, DirEntry):
            return False
        return (self.path, self.size, self.file_count) == (other.path, other.size, other.file_count)


class HashfileBrowser:
    """Simple ncdu-inspired browser for hashfiles."""
    
    def __init__(self):
        self.root_dir: Optional[DirEntry] = None
        self.current_dir: Optional[DirEntry] = None
        self.selected_index = 0
        self.scroll_offset = 0
        # Sorting options
        self.sort_by_size = False  # False = sort by name, True = sort by size
        self.show_size_bars = True  # Show/hide size bars
        # Tagging system
        self.tagged_items = set()  # Set of tagged FileEntry and DirEntry objects
        
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
        
        # Clear screen and position cursor at top
        print("\033[2J\033[H", end="", flush=True)
        
        # Header with breadcrumbs
        breadcrumbs = self.create_breadcrumbs()
        header = f"{Colors.BRIGHT_WHITE}{Colors.BOLD}📁 {breadcrumbs}{Colors.RESET}"

        # Additional safeguard: ensure no double slashes in the final header
        visual_header = self.strip_ansi_codes(header)
        if "//" in visual_header:
            # Fix any double slashes that might have slipped through
            fixed_visual = visual_header.replace("//", "/")
            # Reconstruct header with proper ANSI codes
            # This is a fallback fix for any edge cases
            if visual_header.startswith("📁 //"):
                header = f"{Colors.BRIGHT_WHITE}{Colors.BOLD}📁 {Colors.CYAN}/{Colors.RESET}{Colors.BRIGHT_WHITE}{Colors.BOLD}{fixed_visual[4:]}{Colors.RESET}"

        # Fallback: if breadcrumbs are too short (indicating a problem), show basic path
        visual_breadcrumbs = self.strip_ansi_codes(breadcrumbs)
        if len(visual_breadcrumbs) <= 1 and self.current_dir and self.current_dir.name:
            # Try to construct a basic path representation
            if self.current_dir.name != "(Hash File Root)":
                fallback_breadcrumbs = f"/{self.current_dir.name}"
                header = f"{Colors.BRIGHT_WHITE}{Colors.BOLD}📁 {Colors.BRIGHT_WHITE}{Colors.BOLD}{fallback_breadcrumbs}{Colors.RESET}{Colors.RESET}"

        print(header)
        print("-" * min(width, 80))

        # Get items to display (now using the centralized method)
        items = self.get_current_items()

        # Calculate optimal layout for current directory
        layout = self.calculate_layout(items)

        # Find max size for size bars (excluding parent directory ".." which has size 0)
        max_size = 0
        if self.show_size_bars:
            for item in items:
                name, size, is_dir, obj = item
                if size > max_size:
                    max_size = size
        
        # Calculate display area with buffer for terminal quirks
        display_height = height - 4  # Header + separator + footer + 1 buffer line
        
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
            is_selected = (item_index == self.selected_index)

            # Create line with proper spacing
            marker = ">" if is_selected else " "

            # Get the allocated width for filenames from layout
            name_width = layout['name_width']

            # Always truncate to fit - be more aggressive about this
            # Subtract 2 for padding, but ensure we don't go below minimum readable length
            max_filename_display = max(15, name_width - 2)  # Minimum 15 chars for readability
            truncated_name = self.truncate_filename(name, max_filename_display)

            # Colorize the truncated name and size
            colored_name = self.colorize_item(truncated_name, is_dir, is_selected, obj)
            colored_size = self.colorize_size(size_str)

            if self.show_size_bars and max_size > 0:
                # Include size bar with color
                size_bar = self.create_size_bar(size, max_size, bar_width=10)
                ratio = size / max_size if max_size > 0 else 0
                colored_bar = self.colorize_size_bar(size_bar, ratio)

                # Use calculated layout positions
                name_width = layout['name_width']
                bar_position = layout['bar_position']

                # Build the line with proper visual alignment
                line_parts = [marker, " ", colored_name]
                current_pos = 2 + self.get_visual_length(colored_name)  # marker + space + name

                # Add padding to reach bar position
                while current_pos < bar_position:
                    line_parts.append(" ")
                    current_pos += 1

                line_parts.extend([colored_bar, " ", colored_size])
                line = "".join(line_parts)
            else:
                # No size bar - use dynamic filename width
                name_width = layout['name_width']
                line = f"{marker} {colored_name:<{name_width}} {colored_size:>10}"

            # Final safety check: ensure filename is properly truncated
            visual_name = self.strip_ansi_codes(colored_name)
            if len(visual_name) > max_filename_display:
                # Emergency truncation if something went wrong
                safe_name = self.truncate_filename(truncated_name, max_filename_display)
                colored_name = self.colorize_item(safe_name, is_dir, is_selected, obj)

                # Rebuild the line with the corrected name
                if self.show_size_bars and max_size > 0:
                    size_bar = self.create_size_bar(size, max_size, bar_width=10)
                    ratio = size / max_size if max_size > 0 else 0
                    colored_bar = self.colorize_size_bar(size_bar, ratio)

                    line_parts = [marker, " ", colored_name]
                    current_pos = 2 + self.get_visual_length(colored_name)
                    while current_pos < bar_position:
                        line_parts.append(" ")
                        current_pos += 1
                    line_parts.extend([colored_bar, " ", colored_size])
                    line = "".join(line_parts)
                else:
                    line = f"{marker} {colored_name:<{name_width}} {colored_size:>10}"

            # Handle line formatting based on selection status
            if is_selected:
                # For selected items: apply full-line reverse video highlighting
                visual_line = self.strip_ansi_codes(line)
                padding_needed = width - len(visual_line)
                if padding_needed > 0:
                    line += " " * padding_needed
                line = f"{Colors.REVERSE}{line}{Colors.RESET}"
            else:
                # For non-selected items: truncate to terminal width if needed
                visual_line = self.strip_ansi_codes(line)
                if len(visual_line) > width:
                    # Need to truncate while preserving ANSI codes
                    truncated_visual = visual_line[:width]
                    # Find corresponding position in original string
                    ansi_chars = 0
                    for i, char in enumerate(line):
                        if i - ansi_chars >= len(truncated_visual):
                            line = line[:i]
                            break
                        if char == '\x1b':  # Start of ANSI sequence
                            ansi_chars += 1
            print(line)
        
        # Ultra-compact footer to prevent wrapping
        size_info = f"{Colors.BRIGHT_CYAN}{self.format_size(self.current_dir.size)} ({self.current_dir.file_count}){Colors.RESET}"

        # Add tagged stats if there are tagged items
        tagged_size, tagged_files = self.get_tagged_stats()
        if tagged_files > 0:
            tagged_info = f" | {Colors.BRIGHT_MAGENTA}{self.format_size(tagged_size)} ({tagged_files} tagged){Colors.RESET}"
            size_info += tagged_info

        sort_indicator = "Size" if self.sort_by_size else "Name"

        navigation = f"{Colors.BRIGHT_YELLOW}↑↓←→/hjkl/Home/End{Colors.RESET}"
        actions = f"{Colors.BRIGHT_BLUE}Enter{Colors.RESET}"

        sort_mode = "Size" if self.sort_by_size else "Name"
        bars_status = "ON" if self.show_size_bars else "OFF"

        controls = f"{Colors.BRIGHT_RED}s{Colors.RESET}={sort_mode[:1].lower()}, {Colors.BRIGHT_RED}g{Colors.RESET}={bars_status.lower()}, {Colors.BRIGHT_RED}t{Colors.RESET}/Space=tag, {Colors.BRIGHT_RED}c{Colors.RESET}=clear, {Colors.BRIGHT_RED}q{Colors.RESET}=quit"

        # Very compact single line
        footer = f"{size_info} [{sort_indicator[:1]}] | {navigation}/{actions} | {controls}"
        print(footer)
    
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

    def create_size_bar(self, size: int, max_size: int, bar_width: int = 10) -> str:
        """Create a visual size bar like ncdu."""
        if max_size == 0:
            return ""

        ratio = size / max_size
        filled = int(ratio * bar_width)

        # Use different characters for different fill levels
        if ratio >= 0.9:
            bar_char = "█"
        elif ratio >= 0.7:
            bar_char = "▊"
        elif ratio >= 0.5:
            bar_char = "▌"
        elif ratio >= 0.3:
            bar_char = "▍"
        elif ratio >= 0.1:
            bar_char = "▎"
        else:
            bar_char = "▏"

        bar = bar_char * filled
        remaining = bar_width - filled

        # Add empty space for remaining width
        if remaining > 0:
            bar += " " * remaining

        return f"[{bar}]"

    def get_size_bar_color(self, ratio: float) -> str:
        """Get color for size bar based on size ratio."""
        if ratio >= 0.8:
            return Colors.BRIGHT_RED
        elif ratio >= 0.6:
            return Colors.BRIGHT_YELLOW
        elif ratio >= 0.4:
            return Colors.BRIGHT_GREEN
        elif ratio >= 0.2:
            return Colors.BRIGHT_BLUE
        else:
            return Colors.BRIGHT_BLACK

    def colorize_item(self, name: str, is_dir: bool, is_selected: bool, obj: Optional[object] = None) -> str:
        """Colorize item name based on type, selection status, and tagging."""
        # Check if item is tagged
        is_tagged = obj is not None and obj in self.tagged_items

        if is_selected:
            if is_tagged:
                return f"{Colors.BRIGHT_YELLOW}{name} ✓"
            else:
                return f"{name}"
        elif is_tagged:
            # Tagged items get special coloring and marking
            if is_dir:
                return f"{Colors.BRIGHT_YELLOW}{Colors.BOLD}{name} ✓{Colors.RESET}"
            else:
                return f"{Colors.BRIGHT_YELLOW}{name} ✓{Colors.RESET}"
        elif is_dir:
            return f"{Colors.BRIGHT_BLUE}{Colors.BOLD}{name}{Colors.RESET}"
        else:
            return f"{Colors.WHITE}{name}{Colors.RESET}"

    def colorize_size_bar(self, bar: str, ratio: float) -> str:
        """Colorize size bar with gradient based on size ratio."""
        color = self.get_size_bar_color(ratio)
        return f"{color}{bar}{Colors.RESET}"

    def colorize_size(self, size_str: str) -> str:
        """Colorize file size text."""
        return f"{Colors.BRIGHT_CYAN}{size_str}{Colors.RESET}"

    def create_breadcrumbs(self) -> str:
        """Create breadcrumb trail showing path hierarchy."""
        if not self.current_dir:
            return "/"

        # Build path components
        components = []
        current = self.current_dir

        # Walk up the tree to build the full path
        while current:
            if current.name and current.name != "(Hash File Root)":  # Skip technical names
                components.insert(0, current.name)
            elif not current.parent:  # This is the root
                components.insert(0, "")
            current = current.parent

        # Remove any empty components and clean up
        components = [comp for comp in components if comp]

        # If we have no components, we're at root
        if not components:
            return "/"

        # Create breadcrumb string with proper colors
        breadcrumb_parts = []
        for i, component in enumerate(components):
            if i == len(components) - 1:  # Last component (current directory)
                breadcrumb_parts.append(f"{Colors.BRIGHT_WHITE}{Colors.BOLD}{component}{Colors.RESET}")
            else:  # Parent directories
                breadcrumb_parts.append(f"{Colors.CYAN}{component}{Colors.RESET}")

        full_path = "/".join(breadcrumb_parts)
        # Clean up the path to ensure proper formatting
        full_path = full_path.replace("//", "/")  # Remove double slashes
        if full_path and not full_path.startswith("/"):
            full_path = "/" + full_path  # Ensure absolute path

        # Check if we need to truncate for terminal width
        width, _ = self.get_terminal_size()
        # Reserve space for "📁 " prefix and potential ANSI codes
        # "📁 " is about 4 chars, plus ANSI codes add overhead
        available_space = width - 6  # Conservative estimate for emoji + space + ANSI

        # Get the visual length (excluding ANSI codes)
        visual_length = len(self.strip_ansi_codes(full_path))

        if visual_length <= available_space:
            return full_path

        # Need to truncate - use a simpler, more predictable approach
        current_dir = breadcrumb_parts[-1]
        current_visual = len(self.strip_ansi_codes(current_dir))

        # Reserve space for ellipsis
        ellipsis_space = 3  # "..."

        # If current directory alone is too long, truncate it
        if current_visual >= available_space - ellipsis_space:
            truncated_current = self.strip_ansi_codes(current_dir)
            max_current_len = available_space - ellipsis_space
            if max_current_len > 0:
                truncated_current = "..." + truncated_current[-max_current_len:]
            else:
                truncated_current = "..."

            # Reapply color to truncated current dir
            if Colors.BRIGHT_WHITE in current_dir and Colors.BOLD in current_dir:
                return f"{Colors.BRIGHT_WHITE}{Colors.BOLD}{truncated_current}{Colors.RESET}"
            else:
                return truncated_current

        # Build truncated path by working backwards from current directory
        result_parts = [current_dir]
        remaining_space = available_space - current_visual

        # Add parent directories, starting with the closest ones
        added_ellipsis = False
        for i in range(len(breadcrumb_parts) - 2, -1, -1):
            parent_dir = breadcrumb_parts[i]
            parent_visual = len(self.strip_ansi_codes(parent_dir))
            separator_space = 1  # for "/"

            if not added_ellipsis and parent_visual + separator_space <= remaining_space:
                result_parts.insert(0, parent_dir)
                remaining_space -= parent_visual + separator_space
            elif not added_ellipsis:
                # Add ellipsis and stop adding more directories
                if remaining_space >= ellipsis_space + separator_space:
                    result_parts.insert(0, f"{Colors.DIM}...{Colors.RESET}")
                    added_ellipsis = True
                break
            else:
                break

        final_path = "/".join(result_parts)
        return final_path

    def colorize_header(self, header: str) -> str:
        """Colorize header text."""
        return f"{Colors.BRIGHT_WHITE}{Colors.BOLD}{header}{Colors.RESET}"

    def colorize_footer(self, footer: str) -> str:
        """Colorize footer text."""
        return f"{Colors.DIM}{footer}{Colors.RESET}"

    def strip_ansi_codes(self, text: str) -> str:
        """Remove ANSI escape codes from text for length calculation."""
        import re
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', text)

    def get_visual_length(self, text: str) -> int:
        """Get the visual length of text (excluding ANSI codes)."""
        return len(self.strip_ansi_codes(text))

    def truncate_filename(self, filename: str, max_length: int) -> str:
        """Truncate filename intelligently if too long."""
        if len(filename) <= max_length:
            return filename

        # For very constrained space, prioritize showing the end (extension)
        if max_length <= 12:
            if max_length <= 6:
                # Very constrained, just truncate
                return filename[:max_length]
            else:
                # Show end with "..."
                return "..." + filename[-(max_length-3):]

        # Keep extension if present and we have reasonable space
        if '.' in filename:
            name_part, ext = filename.rsplit('.', 1)
            ext = '.' + ext
            ext_len = len(ext)

            # Reserve space for extension and "..."
            available_for_name = max_length - ext_len - 3  # 3 for "..."

            # Only use extension-preserving truncation if we have enough space for meaningful name
            if available_for_name >= 10:
                truncated_name = name_part[:available_for_name] + "..." + ext
                return truncated_name

        # Fallback: show start and end with "..." in middle
        start_len = max(3, max_length // 2 - 1)
        end_len = max(3, max_length - start_len - 3)
        return filename[:start_len] + "..." + filename[-end_len:]

    def calculate_layout(self, items: List) -> dict:
        """Calculate optimal layout based on content and terminal width."""
        width, height = self.get_terminal_size()

        # Find the longest filename in current directory
        max_name_len = 0
        for name, size, is_dir, obj in items:
            visual_name_len = len(name)  # Raw filename length
            max_name_len = max(max_name_len, visual_name_len)

        # Calculate positions
        marker_space = 2  # ">" + space
        bar_width = 12 if self.show_size_bars else 0  # "[██████████]" + space
        size_width = 10  # Size string width
        min_name_width = 15  # Minimum readable filename width

        # Calculate available space for filename
        available_for_name = width - marker_space - bar_width - size_width - 2  # -2 for spacing

        # Determine optimal filename width based on content and available space
        if available_for_name < min_name_width:
            # Not enough space, disable bars if enabled to gain more space
            if self.show_size_bars:
                self.show_size_bars = False
                bar_width = 0
                available_for_name = width - marker_space - bar_width - size_width - 2

            if available_for_name < min_name_width:
                # Still not enough space, use minimum (will cause truncation)
                optimal_name_width = min_name_width
            else:
                optimal_name_width = available_for_name
        else:
            # Use the full available space - let filenames be as long as possible
            optimal_name_width = available_for_name

        # If we still exceed terminal width, adjust
        total_needed = marker_space + optimal_name_width + bar_width + size_width + 2
        if total_needed > width:
            optimal_name_width = width - marker_space - bar_width - size_width - 2
            optimal_name_width = max(min_name_width, optimal_name_width)

        return {
            'name_width': optimal_name_width,
            'bar_position': marker_space + optimal_name_width,
            'total_width': total_needed
        }

    def get_current_items(self) -> List:
        """Get list of current directory items."""
        items = []

        if self.current_dir.parent:
            items.append(("../", 0, True, None))

        if self.sort_by_size:
            # Sort by size (descending), then by name for ties
            def sort_key_dir(x):
                return (-x.size, x.name.lower())

            def sort_key_file(x):
                return (-x.size, x.path.lower())

            for subdir in sorted(self.current_dir.subdirs, key=sort_key_dir):
                items.append((f"{subdir.name}/", subdir.size, True, subdir))

            for file_entry in sorted(self.current_dir.files, key=sort_key_file):
                filename = os.path.basename(file_entry.path)
                items.append((filename, file_entry.size, False, file_entry))
        else:
            # Sort by name (ascending)
            for subdir in sorted(self.current_dir.subdirs, key=lambda x: x.name.lower()):
                items.append((f"{subdir.name}/", subdir.size, True, subdir))

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

    def toggle_tag_selected(self) -> None:
        """Toggle tag status for the selected item and advance to next item."""
        items = self.get_current_items()
        if not items or self.selected_index >= len(items):
            return

        name, size, is_dir, obj = items[self.selected_index]

        # Skip parent directory ("..")
        if obj is None:
            return

        # Toggle tag status
        if obj in self.tagged_items:
            self.tagged_items.remove(obj)
        else:
            self.tagged_items.add(obj)

        # Auto-advance to next item for easier tagging workflow (don't wrap around)
        items_count = len(items)
        if items_count > 0 and self.selected_index < items_count - 1:
            self.selected_index += 1

    def get_tagged_stats(self) -> Tuple[int, int]:
        """Get total size and file count of tagged items."""
        total_size = 0
        total_files = 0

        for item in self.tagged_items:
            if isinstance(item, DirEntry):
                total_size += item.size
                total_files += item.file_count
            elif isinstance(item, FileEntry):
                total_size += item.size
                total_files += 1

        return total_size, total_files

    def clear_all_taggings(self) -> None:
        """Clear all tagged items."""
        self.tagged_items.clear()
    
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
                elif key in ['\033[C', 'l', '\r', '\n']:  # Right arrow, vim l, or Enter (enter directory)
                    self.enter_selected()
                # Home/End navigation
                elif key in ['\033[H', '\033[1~']:  # Home key - go to first item
                    self.selected_index = 0
                    self.scroll_offset = 0
                elif key in ['\033[F', '\033[4~']:  # End key - go to last item
                    items_count = len(self.get_current_items())
                    if items_count > 0:
                        self.selected_index = items_count - 1
                        # Adjust scroll offset to show the last item
                        width, height = self.get_terminal_size()
                        display_height = height - 4  # Header + separator + footer + 1 buffer line
                        if self.selected_index >= display_height:
                            self.scroll_offset = self.selected_index - display_height + 1
                        else:
                            self.scroll_offset = 0
                # Page navigation - multiple sequences for cross-platform compatibility
                elif key in ['\033[5~', '\033[[5~', '\002', '\x02']:  # Page Up (Ctrl+B)
                    self.navigate_page(-1)
                elif key in ['\033[6~', '\033[[6~', '\006', '\x06']:  # Page Down (Ctrl+F)
                    self.navigate_page(1)
                # Alternative page navigation
                elif key == 'b':  # 'b' - Page Up
                    self.navigate_page(-1)
                # Tagging
                elif key in ['t', ' ']:  # 't' or Space - Toggle tag
                    self.toggle_tag_selected()
                elif key == 'c':  # 'c' - Clear all taggings
                    self.clear_all_taggings()
                # Sorting and display options
                elif key == 's':  # Toggle sorting mode
                    self.sort_by_size = not self.sort_by_size
                    self.selected_index = 0  # Reset selection to top
                    self.scroll_offset = 0
                elif key == 'g':  # Toggle size bars
                    self.show_size_bars = not self.show_size_bars
                    
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
