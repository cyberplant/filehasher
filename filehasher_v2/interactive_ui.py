"""
Interactive full screen UI for filehasher using textual.
"""

import asyncio
import threading
import time
import socket
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Static, ProgressBar, Button, DataTable, Header, Footer,
    Label, TextArea
)
from textual.binding import Binding
from textual.reactive import reactive
from textual import events

from .processor import HashProcessor
from .hash_algorithms import HashAlgorithm


# Message classes for communication between threads
class ProgressUpdateMessage(events.Message):
    def __init__(self, progress_data: Dict[str, Any]):
        self.progress_data = progress_data
        super().__init__()


class HashEntryMessage(events.Message):
    def __init__(self, message: dict):
        self.message = message
        super().__init__()


class ProcessingCompleteMessage(events.Message):
    pass


class ProcessingErrorMessage(events.Message):
    def __init__(self, error: str = "Processing failed"):
        self.error = error
        super().__init__()


@dataclass
class WorkerState:
    """State for a worker process."""
    worker_id: int
    current_file: str = ""
    files_processed: int = 0
    bytes_processed: int = 0
    is_paused: bool = False
    progress_percent: float = 0.0
    queue_files: List[str] = None
    
    def __post_init__(self):
        if self.queue_files is None:
            self.queue_files = []


@dataclass
class GlobalState:
    """Global state for the UI."""
    total_files: int = 0
    total_bytes: int = 0
    files_processed: int = 0
    bytes_processed: int = 0
    is_paused: bool = False
    recent_hashes: List[str] = None
    start_time: float = 0.0
    
    def __post_init__(self):
        if self.recent_hashes is None:
            self.recent_hashes = []
        if self.start_time == 0.0:
            self.start_time = time.time()


class UIProgressListener:
    """UDP listener for progress updates from workers."""
    
    def __init__(self, port: int = 0, ui_app=None):
        self.port = port
        self.sock = None
        self.listening = False
        self.ui_app = ui_app
        self.worker_stats = {}
        self.total_files_processed = 0
        self.total_bytes_processed = 0
        self.current_file_progress = {}
        self.lock = threading.Lock()
    
    def start(self):
        """Start UDP listener."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('localhost', self.port))
            self.port = self.sock.getsockname()[1]  # Get actual port
            self.listening = True
            print(f"DEBUG: UDP listener bound to port {self.port}")
            
            # Start listener thread
            self.listener_thread = threading.Thread(target=self._listen, daemon=True)
            self.listener_thread.start()
            print("DEBUG: Started UDP listener thread")
            
        except Exception as e:
            import traceback
            print(f"DEBUG: Error starting UDP listener: {str(e)}\n{traceback.format_exc()}")
            raise
    
    def stop(self):
        """Stop UDP listener."""
        self.listening = False
        if self.sock:
            self.sock.close()
    
    def _listen(self):
        """Listen for UDP messages."""
        while self.listening:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                self._handle_message(message)
            except (socket.error, json.JSONDecodeError):
                continue
    
    def _handle_message(self, message: dict):
        """Handle incoming progress message."""
        try:
            with self.lock:
                worker_id = message.get('worker_id')
                if worker_id is None:
                    print(f"DEBUG: Message missing worker_id: {message}")
                    return
                
                if worker_id not in self.worker_stats:
                    self.worker_stats[worker_id] = {
                        'files_processed': 0,
                        'bytes_processed': 0,
                        'start_time': time.time()
                    }
                
                if message['message_type'] == 'progress':
                    # File-level progress (file completed)
                    self.worker_stats[worker_id]['files_processed'] = message.get('files_processed', 0)
                    self.worker_stats[worker_id]['bytes_processed'] = message.get('bytes_processed', 0)
                    
                    # Update totals
                    self.total_files_processed = sum(ws['files_processed'] for ws in self.worker_stats.values())
                    self.total_bytes_processed = sum(ws['bytes_processed'] for ws in self.worker_stats.values())
                    
                elif message['message_type'] == 'file_progress':
                    # File-level progress (bytes processed within current file)
                    self.current_file_progress[worker_id] = {
                        'current_file': message.get('current_file', ''),
                        'bytes_processed': message.get('bytes_processed', 0),
                        'file_size': message.get('file_size', 0),
                        'files_processed': message.get('files_processed', 0)
                    }
                
                elif message['message_type'] == 'hash_entry':
                    # Hash entry message - add to recent hashes
                    if self.ui_app:
                        self.ui_app.post_message(HashEntryMessage(message))
            
            # Update UI with current progress
            if self.ui_app:
                self.ui_app.post_message(ProgressUpdateMessage(self.get_progress()))
                
        except Exception as e:
            import traceback
            print(f"DEBUG: Error handling message: {str(e)}\n{traceback.format_exc()}\nMessage: {message}")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress."""
        with self.lock:
            return {
                'files_processed': self.total_files_processed,
                'bytes_processed': self.total_bytes_processed,
                'worker_stats': self.worker_stats.copy(),
                'current_file_progress': self.current_file_progress.copy()
            }


class WorkerPane(Container):
    """Pane showing individual worker status."""
    
    def __init__(self, worker_id: int, **kwargs):
        try:
            print(f"DEBUG: Creating WorkerPane for worker {worker_id}")
            super().__init__(**kwargs)
            self.worker_id = worker_id
            self.state = WorkerState(worker_id=worker_id)
            print(f"DEBUG: WorkerPane {worker_id} created successfully")
        except Exception as e:
            import traceback
            error_msg = f"Error in WorkerPane.__init__ for worker {worker_id}: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            raise
    
    def compose(self) -> ComposeResult:
        with Container(classes="worker-pane"):
            yield Label(f"Worker {self.worker_id}", classes="worker-title")
            yield Static("", id=f"current-file-{self.worker_id}")
            yield ProgressBar(id=f"progress-{self.worker_id}")
            yield Static("", id=f"queue-{self.worker_id}")
            yield Button("Pause", id=f"pause-btn-{self.worker_id}", variant="warning")
    
    def update_state(self, state: WorkerState):
        """Update worker state display."""
        self.state = state
        
        # Update current file
        current_file_widget = self.query_one(f"#current-file-{self.worker_id}", Static)
        current_file_widget.update(f"Processing: {Path(state.current_file).name}")
        
        # Update progress bar
        progress_widget = self.query_one(f"#progress-{self.worker_id}", ProgressBar)
        progress_widget.progress = state.progress_percent
        
        # Update queue
        queue_widget = self.query_one(f"#queue-{self.worker_id}", Static)
        queue_text = "Queue:\n"
        for i, file in enumerate(state.queue_files[:3]):  # Show next 3 files
            queue_text += f"  {i+1}. {Path(file).name}\n"
        queue_widget.update(queue_text)
        
        # Update pause button
        pause_btn = self.query_one(f"#pause-btn-{self.worker_id}", Button)
        pause_btn.label = "Resume" if state.is_paused else "Pause"
        pause_btn.variant = "error" if state.is_paused else "warning"


class GeneralInfoPane(Container):
    """Pane showing overall progress and statistics."""
    
    def compose(self) -> ComposeResult:
        with Container(classes="info-pane"):
            yield Label("Overall Progress", classes="info-title")
            yield Static("", id="total-stats")
            yield ProgressBar(id="overall-progress")
            yield Static("", id="performance-stats")
    
    def update_stats(self, state: GlobalState):
        """Update general statistics."""
        # Total stats
        total_widget = self.query_one("#total-stats", Static)
        total_widget.update(
            f"Files: {state.files_processed}/{state.total_files} | "
            f"Bytes: {self._format_bytes(state.bytes_processed)}/{self._format_bytes(state.total_bytes)}"
        )
        
        # Overall progress
        progress_widget = self.query_one("#overall-progress", ProgressBar)
        if state.total_files > 0:
            progress_widget.progress = state.files_processed / state.total_files
        
        # Performance stats
        perf_widget = self.query_one("#performance-stats", Static)
        elapsed = time.time() - state.start_time
        if elapsed > 0:
            files_per_sec = state.files_processed / elapsed
            mb_per_sec = (state.bytes_processed / (1024 * 1024)) / elapsed
            perf_widget.update(
                f"Speed: {files_per_sec:.1f} files/sec, {mb_per_sec:.1f} MB/sec"
            )
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"


class HashOutputPane(Container):
    """Pane showing recent hash output."""
    
    def compose(self) -> ComposeResult:
        with Container(classes="hash-pane"):
            yield Label("Recent Hash Output", classes="hash-title")
            yield TextArea(id="hash-output", read_only=True)
    
    def add_hash(self, hash_line: str):
        """Add a new hash line to the output."""
        hash_widget = self.query_one("#hash-output", TextArea)
        current_text = hash_widget.text
        new_text = f"{hash_line}\n{current_text}"
        # Keep only last 50 lines
        lines = new_text.split('\n')
        if len(lines) > 50:
            lines = lines[:50]
        hash_widget.text = '\n'.join(lines)


class FileHasherUI(App):
    """Main interactive UI application."""
    
    CSS = """
    .worker-pane {
        border: solid $primary;
        margin: 1;
        padding: 1;
        height: 20;
    }
    
    .info-pane {
        border: solid $secondary;
        margin: 1;
        padding: 1;
        height: 15;
    }
    
    .hash-pane {
        border: solid $accent;
        margin: 1;
        padding: 1;
        height: 20;
    }
    
    .worker-title, .info-title, .hash-title {
        text-style: bold;
        color: $primary;
    }
    
    #hash-output {
        background: $surface;
        color: $text;
    }
    """
    
    BINDINGS = [
        Binding("p", "pause_all", "Pause All", show=True),
        Binding("r", "resume_all", "Resume All", show=True),
        Binding("q", "quit", "Quit", show=True),
    ]
    
    def __init__(self, directory: str, output_file: str, algorithm: HashAlgorithm, 
                 num_workers: int, **kwargs):
        try:
            print(f"DEBUG: Initializing FileHasherUI with num_workers={num_workers}")
            super().__init__(**kwargs)
            self.directory = directory
            self.output_file = output_file
            self.algorithm = algorithm
            self.num_workers = num_workers
            
            self.global_state = GlobalState()
            self.worker_states: Dict[int, WorkerState] = {}
            self.processor: Optional[HashProcessor] = None
            self.processing_thread: Optional[threading.Thread] = None
            self._is_running = False
            self.progress_listener: Optional[UIProgressListener] = None
            
            # Initialize worker states
            print(f"DEBUG: Creating {num_workers} worker states")
            for i in range(num_workers):
                self.worker_states[i] = WorkerState(worker_id=i)
            print("DEBUG: FileHasherUI initialization complete")
            
        except Exception as e:
            import traceback
            error_msg = f"Error in FileHasherUI.__init__: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            raise
    
    def compose(self) -> ComposeResult:
        try:
            print(f"DEBUG: Composing UI with {self.num_workers} workers")
            yield Header()
            
            with Horizontal():
                # Worker panes
                for i in range(self.num_workers):
                    print(f"DEBUG: Creating worker pane {i}")
                    yield WorkerPane(worker_id=i)
            
            with Horizontal():
                print("DEBUG: Creating info and hash panes")
                yield GeneralInfoPane()
                yield HashOutputPane()
            
            yield Footer()
            print("DEBUG: UI composition complete")
            
        except Exception as e:
            import traceback
            error_msg = f"Error in compose: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            raise
    
    def on_mount(self) -> None:
        """Start processing when UI is mounted."""
        self.start_processing()
    
    def start_processing(self):
        """Start the hash processing in a background thread."""
        try:
            self._is_running = True
            
            # Start UI progress listener
            self.progress_listener = UIProgressListener(ui_app=self)
            self.progress_listener.start()
            print(f"DEBUG: Started progress listener on port {self.progress_listener.port}")
            
            # Start processing thread
            self.processing_thread = threading.Thread(target=self._run_processing, daemon=True)
            self.processing_thread.start()
            print("DEBUG: Started processing thread")
            
        except Exception as e:
            import traceback
            error_msg = f"Error in start_processing: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            self.post_message(ProcessingErrorMessage(error_msg))
    
    def _run_processing(self):
        """Run the hash processing."""
        try:
            # Set the UDP port for the processor to use
            import os
            if self.progress_listener and self.progress_listener.port:
                os.environ['FILEHASHER_UDP_PORT'] = str(self.progress_listener.port)
                print(f"DEBUG: Set UDP port to {self.progress_listener.port}")
            else:
                print("DEBUG: No progress listener or port available")
                self.app.post_message(ProcessingErrorMessage("Failed to initialize progress listener"))
                return
            
            self.processor = HashProcessor(algorithm=self.algorithm, num_workers=self.num_workers)
            
            # Process directory
            success = self.processor.process_directory(
                self.directory,
                self.output_file,
                follow_symlinks=False,
                quiet=True,
                create_new_file=True,
                debug=False,
                update_mode=False,
                ignore_mtime=False
            )
            
            if success:
                self.app.post_message(ProcessingCompleteMessage())
            else:
                self.app.post_message(ProcessingErrorMessage())
                
        except Exception as e:
            import traceback
            error_msg = f"Error in _run_processing: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            self.app.post_message(ProcessingErrorMessage(error_msg))
    
    def action_pause_all(self):
        """Pause all workers."""
        self.global_state.is_paused = True
        for worker_state in self.worker_states.values():
            worker_state.is_paused = True
        self.update_displays()
    
    def action_resume_all(self):
        """Resume all workers."""
        self.global_state.is_paused = False
        for worker_state in self.worker_states.values():
            worker_state.is_paused = False
        self.update_displays()
    
    def action_quit(self):
        """Quit the application."""
        self._is_running = False
        if self.processor:
            self.processor.stop()
        if self.progress_listener:
            self.progress_listener.stop()
        self.exit()
    
    def update_displays(self):
        """Update all display widgets."""
        # Update worker panes
        for i, worker_pane in enumerate(self.query(WorkerPane)):
            if i in self.worker_states:
                worker_pane.update_state(self.worker_states[i])
        
        # Update general info
        info_pane = self.query_one(GeneralInfoPane)
        info_pane.update_stats(self.global_state)
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        if event.button.id.startswith("pause-btn-"):
            worker_id = int(event.button.id.split("-")[-1])
            if worker_id in self.worker_states:
                self.worker_states[worker_id].is_paused = not self.worker_states[worker_id].is_paused
                self.update_displays()
    
    def on_progress_update(self, event: ProgressUpdateMessage) -> None:
        """Handle progress updates."""
        progress_data = event.progress_data
        
        # Update global state
        self.global_state.files_processed = progress_data['files_processed']
        self.global_state.bytes_processed = progress_data['bytes_processed']
        
        # Update worker states
        worker_stats = progress_data.get('worker_stats', {})
        current_file_progress = progress_data.get('current_file_progress', {})
        
        for worker_id, stats in worker_stats.items():
            if worker_id in self.worker_states:
                self.worker_states[worker_id].files_processed = stats['files_processed']
                self.worker_states[worker_id].bytes_processed = stats['bytes_processed']
                
                # Update current file
                if worker_id in current_file_progress:
                    file_progress = current_file_progress[worker_id]
                    self.worker_states[worker_id].current_file = file_progress['current_file']
                    
                    # Calculate progress percentage
                    file_size = file_progress.get('file_size', 0)
                    if file_size > 0:
                        bytes_processed = file_progress.get('bytes_processed', 0)
                        self.worker_states[worker_id].progress_percent = bytes_processed / file_size
        
        self.update_displays()
    
    def on_hash_entry(self, event: HashEntryMessage) -> None:
        """Handle hash entry messages."""
        message = event.message
        
        # Format hash entry for display
        relative_path = Path(message['relative_path'])
        filename = relative_path.name
        hash_value = message['file_hash']
        
        # Add algorithm prefix if not MD5
        if self.algorithm != HashAlgorithm.MD5:
            hash_value = f"{self.algorithm.value.upper()}:{hash_value}"
        
        hash_line = f"{hash_value} | {filename}"
        
        # Add to hash output pane
        hash_pane = self.query_one(HashOutputPane)
        hash_pane.add_hash(hash_line)
    
    def on_processing_complete(self, event: ProcessingCompleteMessage) -> None:
        """Handle processing completion."""
        self.notify("Processing completed successfully!", severity="information")
    
    def on_processing_error(self, event: ProcessingErrorMessage) -> None:
        """Handle processing errors."""
        self.notify(f"Processing error: {event.error}", severity="error")


def run_interactive_ui(directory: str, output_file: str, algorithm: HashAlgorithm, 
                      num_workers: int) -> int:
    """Run the interactive UI."""
    try:
        print(f"DEBUG: Starting interactive UI with directory={directory}, output={output_file}, algorithm={algorithm}, workers={num_workers}")
        app = FileHasherUI(directory, output_file, algorithm, num_workers)
        print("DEBUG: FileHasherUI created successfully")
        result = app.run()
        print(f"DEBUG: App finished with result: {result}")
        return result
    except Exception as e:
        import traceback
        error_msg = f"Error in run_interactive_ui: {str(e)}\n{traceback.format_exc()}"
        print(f"DEBUG: {error_msg}")
        return 1