"""
Architectural Layer Logger
==========================
Provides colored, timestamped, layered output that visually demonstrates
the 3-tier Client → Server → Peer architecture of the distributed file system.

Layers:
  [CLIENT]  — Local operations on this machine (cyan)
  [SERVER ↑] — Request sent to Central Metadata Hub (blue)
  [SERVER ↓] — Response received from Central Metadata Hub (bright blue)
  [PEER  ↑] — Data sent directly to a remote peer node (green)
  [PEER  ↓] — Data received directly from a remote peer node (bright green)
  [ERROR]   — Any failure at any layer (red)
  [✓ DONE]  — Successful completion of an operation (bold green)
"""

import time
from datetime import datetime

# ── ANSI Color Codes ──────────────────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"

# Text colors
CYAN    = "\033[36m"
BLUE    = "\033[34m"
BBLUE   = "\033[94m"   # Bright blue
GREEN   = "\033[32m"
BGREEN  = "\033[92m"   # Bright green
YELLOW  = "\033[33m"
RED     = "\033[91m"
WHITE   = "\033[97m"
MAGENTA = "\033[35m"

# Layer tag definitions:  (label_text, color, arrow_color)
_LAYERS = {
    "CLIENT":    (f"{CYAN}{BOLD}CLIENT   {RESET}", CYAN),
    "SERVER_UP": (f"{BLUE}{BOLD}SERVER ↑ {RESET}", BLUE),
    "SERVER_DN": (f"{BBLUE}{BOLD}SERVER ↓ {RESET}", BBLUE),
    "PEER_UP":   (f"{GREEN}{BOLD}PEER   ↑ {RESET}", GREEN),
    "PEER_DN":   (f"{BGREEN}{BOLD}PEER   ↓ {RESET}", BGREEN),
    "ERROR":     (f"{RED}{BOLD}ERROR    {RESET}", RED),
    "DONE":      (f"{BGREEN}{BOLD}✓ DONE   {RESET}", BGREEN),
    "INFO":      (f"{YELLOW}{BOLD}INFO     {RESET}", YELLOW),
}

# Global verbose flag — True by default, set to False with --quiet
_verbose = True

def set_verbose(flag: bool):
    global _verbose
    _verbose = flag

def is_verbose() -> bool:
    return _verbose

def _timestamp() -> str:
    return f"{DIM}{datetime.now().strftime('%H:%M:%S')}{RESET}"

def _bar() -> str:
    """Print a subtle horizontal separator."""
    return f"{DIM}{'─' * 68}{RESET}"

def log(layer: str, message: str):
    """
    Emit an architectural trace log line.

    Args:
        layer:   One of CLIENT | SERVER_UP | SERVER_DN | PEER_UP | PEER_DN | ERROR | DONE | INFO
        message: Human-readable description of what is happening at this layer.
    """
    if not _verbose:
        return
    label, color = _LAYERS.get(layer, (_LAYERS["INFO"]))
    ts = _timestamp()
    print(f"  {ts}  [{label}]  {color}{message}{RESET}")

def section(title: str):
    """Print a bold section header to visually separate operations."""
    if not _verbose:
        return
    print()
    print(f"  {_bar()}")
    print(f"  {BOLD}{WHITE}  ❯ {title}{RESET}")
    print(f"  {_bar()}")

def result(success: bool, message: str):
    """Print the final outcome of an operation."""
    if not _verbose:
        return
    if success:
        print(f"\n  {BGREEN}{BOLD}  ✓ {message}{RESET}")
    else:
        print(f"\n  {RED}{BOLD}  ✗ {message}{RESET}")
    print()
