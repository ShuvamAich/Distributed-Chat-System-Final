"""
Rich terminal logging with timestamps, color-coding, and structured output.
Designed for demo visibility — every significant system event is logged with
enough detail to demonstrate ordering, elections, fault tolerance, etc.
"""

import time
import threading
from datetime import datetime


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"


CATEGORY_COLORS = {
    "DISCOVERY": Colors.CYAN,
    "ELECTION": Colors.MAGENTA,
    "HEARTBEAT": Colors.GRAY,
    "MESSAGE": Colors.GREEN,
    "ORDER": Colors.YELLOW,
    "FAULT": Colors.RED,
    "VIEW": Colors.BLUE,
    "SYNC": Colors.CYAN,
    "SYSTEM": Colors.WHITE,
    "RELIABILITY": Colors.YELLOW,
}


class SystemLogger:
    """Thread-safe logger with rich formatting for demo purposes."""

    def __init__(self, node_id, role="SERVER"):
        self.node_id = node_id
        self.role = role
        self._lock = threading.Lock()
        self._start_time = time.time()

    def _format_timestamp(self):
        now = datetime.now()
        elapsed = time.time() - self._start_time
        return f"{now.strftime('%H:%M:%S.%f')[:-3]} [+{elapsed:7.3f}s]"

    def _log(self, category, message, detail=None):
        color = CATEGORY_COLORS.get(category, Colors.WHITE)
        ts = self._format_timestamp()
        with self._lock:
            header = (f"{Colors.GRAY}{ts}{Colors.RESET} "
                      f"{color}[{category:^11}]{Colors.RESET} "
                      f"{Colors.BOLD}[{self.role}:{self.node_id}]{Colors.RESET}")
            print(f"{header} {message}")
            if detail:
                padding = " " * 36
                print(f"{padding}{Colors.GRAY}{detail}{Colors.RESET}")

    def discovery(self, message, detail=None):
        self._log("DISCOVERY", message, detail)

    def election(self, message, detail=None):
        self._log("ELECTION", message, detail)

    def heartbeat(self, message, detail=None):
        self._log("HEARTBEAT", message, detail)

    def message(self, message, detail=None):
        self._log("MESSAGE", message, detail)

    def order(self, message, detail=None):
        self._log("ORDER", message, detail)

    def fault(self, message, detail=None):
        self._log("FAULT", message, detail)

    def view(self, message, detail=None):
        self._log("VIEW", message, detail)

    def sync(self, message, detail=None):
        self._log("SYNC", message, detail)

    def system(self, message, detail=None):
        self._log("SYSTEM", message, detail)

    def reliability(self, message, detail=None):
        self._log("RELIABILITY", message, detail)

    def banner(self, text):
        with self._lock:
            border = "=" * 60
            print(f"\n{Colors.BOLD}{Colors.CYAN}{border}")
            print(f"  {text}")
            print(f"{border}{Colors.RESET}\n")

    def separator(self):
        with self._lock:
            print(f"{Colors.GRAY}{'─' * 60}{Colors.RESET}")
