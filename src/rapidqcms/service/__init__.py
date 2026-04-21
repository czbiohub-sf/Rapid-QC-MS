"""QC service: pipeline orchestration, gate file events, and file-system watchers.

Re-exports keep existing callers working after the internal reorganisation
into ``events/`` and ``watchers/`` sub-packages.
"""

from .events.gating import get_gate_status, read_gate_file, write_gate_file
from .watchers.listener import ListenerConfig, start_listener
from .watchers.mzml_watcher import MzmlWatcherConfig, start_mzml_watcher
from .watchers.watcher import start_watcher

__all__ = [
    "write_gate_file",
    "read_gate_file",
    "get_gate_status",
    "start_listener",
    "ListenerConfig",
    "start_watcher",
    "start_mzml_watcher",
    "MzmlWatcherConfig",
]
