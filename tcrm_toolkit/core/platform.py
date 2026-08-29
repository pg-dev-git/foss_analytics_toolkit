"""Cross-platform utilities for OS detection and paths."""

import os
import sys
import platform
from pathlib import Path
from typing import Literal

OSType = Literal["windows", "linux", "darwin"]


def get_os() -> OSType:
    """Detect current operating system."""
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    elif system == "darwin":
        return "darwin"
    return "linux"


def get_config_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate config directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    
    path = base / app_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_data_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate data directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    
    path = base / app_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cache_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate cache directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "Cache"
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    
    path = base / app_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def is_windows() -> bool:
    return get_os() == "windows"


def is_macos() -> bool:
    return get_os() == "darwin"


def is_linux() -> bool:
    return get_os() == "linux"


def get_terminal_size() -> tuple[int, int]:
    """Get terminal size cross-platform."""
    try:
        import shutil
        return shutil.get_terminal_size()
    except Exception:
        return (80, 24)


def supports_true_color() -> bool:
    """Check if terminal supports true color."""
    colorterm = os.environ.get("COLORTERM", "").lower()
    return "truecolor" in colorterm or "24bit" in colorterm
