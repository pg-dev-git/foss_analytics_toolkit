# CRM Toolkit Interactive TUI - User Guide

**Comprehensive Guide** — Installation, usage, configuration, and troubleshooting for the Interactive TUI with VPN/Proxy Safety Monitor.

---

## 🚀 Quick Start

```bash
# 1. Install with interactive extras
uv pip install -e ".[interactive,dev]"

# 2. Run TUI
tcrm                    # Interactive mode
tcrm doctor             # System diagnostics
```

## 🎮 Navigation & Keybindings

### Global Shortcuts
- **Ctrl+Q**: Quit application
- **Ctrl+P**: Command palette (fuzzy search actions)
- **Ctrl+O**: Organization picker (switch org)
- **Ctrl+R**: Refresh current view
- **F1**: Help screen
- **Escape**: Back / Cancel / Clear search

### Navigation & Browsing
- **Tab / Shift+Tab**: Move between panels
- **Arrow Up / Down / j / k**: Navigate lists
- **Page Up / Page Down**: Scroll pages
- **Home / End**: Start/End of list
- **Enter**: Select / Activate item

### Data Browsers
- **`/`**: Focus search input
- **`Escape`**: Clear search
- **`Enter`**: Apply search filter
- **`Click Header`**: Sort column

### Actions (when row selected)
- **`E`**: Extract dataset
- **`U`**: Upload dataset
- **`B`**: Backup dashboard
- **`R`**: Restore dashboard from backup
- **`S`**: Start dataflow execution
- **`T`**: Stop running dataflow
- **`Y`**: Show dependencies (what uses this item)
- **`D`**: Delete item (with confirmation)
- **`C`**: Copy ID to clipboard

---

## ⚙️ Configuration

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `TCRM_TUI_THEME` | Theme: dark, light, auto | dark |
| `TCRM_TUI_KEYBINDINGS` | Keybinding style: vim, standard | standard |
| `TCRM_TUI_AUTO_REFRESH_INTERVAL` | Job monitor poll interval (seconds) | 10 |
| `TCRM_TUI_MAX_HISTORY_ITEMS` | Max task history items | 100 |
| `TCRM_TUI_BROWSER_PAGE_SIZE` | Items per page in browsers | 50 |
| `SAFETY_CHECK_ENABLED` | Enable VPN/Proxy detection | true |
| `SAFETY_CHECK_INTERVAL` | Seconds between safety checks | 300 |
| `SAFETY_BLOCK_ON_CRITICAL` | Block API calls on critical risk | true |
| `SAFETY_ALLOWLIST_IPS` | Comma-separated IPs to skip checks | (empty) |

### Persistent Settings
Automatically saved to:
- `~/.tcrm/config.json` (TUI settings)
- `~/.tcrm/window_state.json` (window state & preferences)
- `~/.tcrm/history.json` (task history)

---

## 🛡️ Connection Safety Monitor
Salesforce immediately disables users detected on VPN/Proxy. The continuous safety monitor:
- **🟢 Green**: Connection safe
- **🟡 Yellow**: Warning (system proxy set but IP clean)
- **🔴 Red**: Critical (VPN/Proxy/Tor detected) — blocks Salesforce API calls and prompts modal warning.

---

## 🔧 Diagnostics
Run the built-in diagnostic tool:
```bash
tcrm doctor
```
Checks Python version, SF CLI installation, keyring access, directory permissions, and connection safety.
