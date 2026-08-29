"""Re-export platform utilities."""
from tcrm_toolkit.core.platform import (
    OSType,
    get_cache_dir,
    get_config_dir,
    get_data_dir,
    get_os,
    get_terminal_size,
    is_linux,
    is_macos,
    is_windows,
    supports_true_color,
)

__all__ = [
    "get_os",
    "get_config_dir",
    "get_data_dir",
    "get_cache_dir",
    "is_windows",
    "is_macos",
    "is_linux",
    "get_terminal_size",
    "supports_true_color",
    "OSType",
]
