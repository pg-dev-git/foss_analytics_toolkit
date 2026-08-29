# Architecture Decision Log

This document records key architectural decisions made during the development of the CRM Toolkit Interactive TUI.

## Decision Format

Each decision follows this structure:
- **ID**: Unique identifier (ADR-001, ADR-002, etc.)
- **Title**: Short, descriptive title
- **Status**: Proposed | Accepted | Superseded | Deprecated
- **Context**: The problem or situation that led to this decision
- **Decision**: The chosen solution
- **Consequences**: Positive, negative, and neutral effects
- **Related Decisions**: Any decisions that are related or affected

---

## ADR-001: Use Textual for TUI Framework
- **Status**: Accepted
- **Context**: Need a modern, async TUI framework that integrates well with existing async codebase
- **Decision**: Use Textual (by Textualize) as the TUI framework
- **Consequences**:
  - + Modern, React-like component model
  - + CSS-based styling and theming
  - + Excellent async support
  - + Active development and community
  - - Learning curve for team unfamiliar with Textual
  - - Additional dependency (~2MB)
- **Related Decisions**: ADR-002 (CSS theming), ADR-006 (Widget composition)

---

## ADR-002: CSS-Based Theming System
- **Status**: Accepted
- **Context**: Need to support dark/light themes and customization
- **Decision**: Use CSS files for theming with CSS variables for easy customization
- **Consequences**:
  - + Separation of concerns (structure vs styling)
  - + Easy theme switching at runtime
  - + Support for custom themes via CSS overrides
  - + Familiar to web developers
  - - Requires maintaining multiple CSS files
  - - Slightly larger CSS payload
- **Related Decisions**: ADR-001 (Textual framework), ADR-004 (Configuration persistence)

---

## ADR-003: SF CLI Authentication as Mandatory Default
- **Status**: Accepted
- **Context**: User requires SF CLI auth as primary method due to VPN/Proxy restrictions and ease of use
- **Decision**: Make SF CLI authentication the default and recommended method, with other auth methods (JWT, Web PKCE, Device) available as CLI-only options
- **Consequences**:
  - + No Connected App required for users
  - + Web flow is user-friendly
  - + Token storage and auto-refresh handled by SF CLI
  - + Works reliably across networks
  - - Requires SF CLI installation
  - - Less flexible for server-to-server automation (though JWT still available)
- **Related Decisions**: ADR-009 (Safety monitor), ADR-012 (Session manager)

---

## ADR-004: Pydantic Settings for Configuration
- **Status**: Accepted
- **Context**: Need type-safe, environment-variable configurable settings for TUI
- **Decision**: Use Pydantic Settings with env var prefix `TCRM_TUI_` for all TUI-specific configuration
- **Consequences**:
  - + Type safety and validation
  - + Automatic env var loading
  - + Good error messages
  - + Integration with Pydantic models throughout codebase
  - - Slight learning curve for Pydantic
  - - Dependency on pydantic-settings
- **Related Decisions**: ADR-002 (Theming), ADR-005 (Config persistence), ADR-006 (TUI Config model)

---

## ADR-005: JSON File Persistence in ~/.tcrm/
- **Status**: Accepted
- **Context**: Need to persist TUI configuration, window state, column preferences, and history
- **Decision**: Store all persistent data in JSON files under `~/.tcrm/` directory
- **Consequences**:
  - + Human-readable and editable
  - + Easy to backup/migrate
  - + Works across all platforms (Windows/Linux/macOS)
  - + Simple implementation
  - - Potential for corruption if written during crash
  - - No built-in querying capabilities
- **Related Decisions**: ADR-004 (Pydantic Settings), ADR-006 (Config Manager), ADR-007 (Window Manager)

---

## ADR-006: Generic DataBrowser Widget
- **Status**: Accepted
- **Context**: Need reusable component for browsing datasets, dashboards, dataflows with common features (search, sort, paginate, select)
- **Decision**: Create a generic `DataBrowser` widget parameterized by column configuration and data loading functions
- **Consequences**:
  - + DRY principle - single implementation for all browsers
  - + Easy to add new browser types
  - + Consistent UX across all data views
  - - Slightly more abstract than concrete implementations
  - - Requires careful interface design
- **Related Decisions**: ADR-007 (Dataset operations), ADR-008 (Dashboard operations), ADR-009 (Dataflow operations)

---

## ADR-007: Client-Side Pagination for Data Browsers
- **Status**: Accepted
- **Context**: Existing service layer doesn't support server-side pagination with offset/limit
- **Decision**: Implement client-side pagination on loaded data (fetch page_size=1000, then paginate client-side)
- **Consequences**:
  - + Works with existing service layer
  - + Fast for <5000 items
  - + Simple implementation
  - - Memory usage grows with total items
  - - Network overhead for fetching all items
  - - Not suitable for >10,000 items without service enhancement
- **Related Decisions**: ADR-006 (DataBrowser), ADR-010 (Future service enhancements)

---

## ADR-008: ProcessPoolExecutor for CPU-Bound Work
- **Status**: Accepted
- **Context**: Need to handle CPU-intensive operations (pandas concat, CSV processing) without blocking UI
- **Decision**: Use `ProcessPoolExecutor` from `concurrent.futures` for CPU-bound work, asyncio for I/O-bound work
- **Consequences**:
  - + Utilizes multiple cores for CPU-intensive tasks
  - + Prevents UI freezing during long operations
  - + Matches legacy multiprocessing performance
  - - Serialization/deserialization overhead
  - - Only works with picklable functions
  - - Memory duplication between processes
- **Related Decisions**: ADR-010 (TaskRunner), ADR-011 (Parallel extraction/upload)

---

## ADR-009: Connection Safety Monitor with Hard Block
- **Status**: Accepted
- **Context**: Salesforce now immediately disables users detected on VPN/Proxy - career ending risk
- **Decision**: Implement background safety monitor that checks for VPN/Proxy/Tor and hard blocks Salesforce API calls when critical risk detected
- **Consequences**:
  - + Protects users from accidental org lockout
  - + Runs continuously in background
  - + Provides clear warnings and modal dialogs
  - - Potential for false positives (mitigated by allowlist)
  - - Slight performance overhead from periodic checks
- **Related Decisions**: ADR-003 (SF CLI Auth), ADR-012 (Session Manager integration), ADR-013 (Safety modals)

---

## ADR-010: TaskRunner with Progress Tracking
- **Status**: Accepted
- **Context**: Need to run background operations with progress updates and cancellation support
- **Decision**: Create `TaskRunner` class that manages async tasks, provides progress messages, and integrates with ProcessPoolExecutor for CPU work
- **Consequences**:
  - + Non-blocking long operations
  - + Real-time progress updates to UI
  - + Cancellation support
  - + Task history and persistence
  - - Increased complexity
  - - Need to handle task lifecycle carefully
- **Related Decisions**: ADR-008 (ProcessPool), ADR-011 (Parallel ops), ADR-014 (Progress panel)

---

## ADR-011: Parallel Dataset Extraction/Upload
- **Status**: Accepted
- **Context**: Need to match or exceed legacy multiprocessing performance for large dataset operations
- **Decision**: Implement parallel extraction (async SAQL queries + process pool merge) and parallel upload (process pool chunk encoding + sequential upload)
- **Consequences**:
  - + Significant performance improvement for large datasets
  - + Efficient use of system resources (I/O async, CPU parallel)
  - + Progress tracking throughout
  - - Complexity in implementation
  - - Temporary disk space for chunks
  - - Need for careful error handling and cleanup
- **Related Decisions**: ADR-008 (ProcessPool), ADR-010 (TaskRunner), ADR-013 (Operations integration)

---

## ADR-012: SessionManager Wraps SFCLIAuthService
- **Status**: Accepted
- **Context**: Need to manage authenticated sessions across TUI lifecycle with multi-org support
- **Decision**: Create `SessionManager` that wraps `SFCLIAuthService` to provide org switching, token caching, and safety integration
- **Consequences**:
  - + Clean separation of concerns
  - + Reusable across TUI and potential CLI usage
  - + Handles multi-org seamlessly
  - + Integrates with safety monitor
  - - Small abstraction layer
  - - Slight indirection
- **Related Decisions**: ADR-003 (SF CLI Auth), ADR-009 (Safety), ADR-013 (Main app integration)

---

## ADR-013: Modal Screens for Transient Interactions
- **Status**: Accepted
- **Context**: Need for login, org picker, safety warnings, and help screens that appear temporarily
- **Decision**: Use Textual `ModalScreen` for all transient interactive screens
- **Consequences**:
  - + Clean separation from main UI
  - + Automatic focus management
  - + Consistent presentation
  - + Easy to dismiss/return
  - - Slight overhead of screen stack management
- **Related Decisions**: ADR-001 (Textual), ADR-003 (Login), ADR-004 (Org picker), ADR-009 (Safety modal), ADR-014 (Help screen)

---

## ADR-014: Tabbed Interface for Help and History
- **Status**: Accepted
- **Context**: Need to organize large amounts of information (help, history) in limited screen space
- **Decision**: Use `TabbedContent` and `TabPane` for organizing help screens, history views, and other tabbed information
- **Consequences**:
  - + Efficient use of screen real estate
  - + Familiar UI pattern
  - + Easy to add/remove tabs
  - - Requires adequate tab labels
  - - Hidden content until tab selected
- **Related Decisions**: ADR-006 (Generic components), ADR-015 (Help screen), ADR-016 (History panel)

---

## ADR-015: Notification Manager with History and Severity
- **Status**: Accepted
- **Context**: Need consistent, user-friendly notifications with history and severity levels
- **Decision**: Create `NotificationManager` wrapper around `app.notify()` that adds history tracking, severity levels, and sticky notifications
- **Consequences**:
  - + Consistent user experience
  - + Notification history for debugging
  - + Configurable timeout and stickiness
  - + Integration with logging
  - - Slight abstraction over built-in notifications
- **Related Decisions**: ADR-004 (Configuration), ADR-016 (Doctor command), ADR-017 (Error handling)

---

## ADR-016: Enhanced Doctor Command
- **Status**: Accepted
- **Context**: Need comprehensive system diagnostics that include TUI-specific checks
- **Decision**: Enhance `tcrm doctor` command to run all system checks in parallel and display results in a clear table format
- **Consequences**:
  - + Comprehensive system validation
  - + Parallel execution for speed
  - + Clear pass/fail reporting
  - + Includes TUI-specific checks (themes, config, safety)
  - - Longer execution time than basic doctor
  - - Potential for cascading failures
- **Related Decisions**: ADR-009 (Safety check), ADR-003 (SF CLI), ADR-004 (Dependencies), ADR-015 (Notifications)

---

## ADR-017: Centralized Error Handling with User Messages
- **Status**: Accepted
- **Context**: Need to present technical errors in user-friendly way while logging details for debugging
- **Decision**: Implement centralized error handling that logs technical details and shows user-friendly messages with suggested actions
- **Consequences**:
  - + Better user experience
  - + Consistent error presentation
  - + Logging preserved for debugging
  - + Actionable guidance for users
  - - Requires discipline to use consistently
  - - Potential for over-abstraction
- **Related Decisions**: ADR-015 (Notifications), ADR-010 (TaskRunner errors), ADR-018 (Validation)

---

## ADR-018: Input Validation and Sanitization
- **Status**: Accepted
- **Context**: Need to protect against injection attacks and malformed input while maintaining usability
- **Decision**: Implement validation at boundaries (API inputs, file paths, user input) with sanitization where appropriate
- **Consequences**:
  - + Improved security
  - + Better data quality
  - + Clear error messages for invalid input
  - - Slight performance overhead
  - - Potential for over-validation
- **Related Decisions**: ADR-017 (Error handling), ADR-006 (DataBrowser validation), ADR-011 (Operation validation)

---

## ADR-019: Internationalization Ready (Future)
- **Status**: Proposed
- **Context**: Potential future need for multi-language support
- **Decision**: Design all user-facing strings to be easily extractable for i18n, but don't implement full i18n in MVP
- **Consequences**:
  - + Easy to add i18n later
  - + Minimal overhead now
  - - Slightly more verbose string handling
  - - No actual translations in MVP
- **Related Decisions**: ADR-002 (Theming - could extend to RTL), ADR-014 (Help screen), ADR-015 (Notifications)

---

## ADR-020: Plugin System Architecture (Future)
- **Status**: Proposed
- **Context**: Need for extensibility to support custom operations and integrations
- **Decision**: Design plugin system using `importlib.metadata` entry points with well-defined hooks and permissions
- **Consequences**:
  - + Enables community contributions
  - + Allows customization without fork
  - + Clear extension points
  - - Security considerations
  - - Complexity in implementation
  - - Versioning challenges
- **Related Decisions**: ADR-001 (Textual - could extend widgets), ADR-010 (TaskRunner - background ops), ADR-013 (Modal screens - plugin UI)

---

## ADR-021: Use `uv` for Dependency Management in Docker
- **Status**: Accepted
- **Context**: Fast, reliable dependency installation required in container builds
- **Decision**: Use Astral `uv` in multi-stage Docker builds
- **Consequences**:
  - + Extremely fast dependency resolution and installation
  - + Consistent python environment reproduction
  - - Requires uv binary in builder stage
- **Related Decisions**: ADR-004 (Dependencies)

---

## ADR-022: Multi-stage Docker Build for Minimal Runtime Image
- **Status**: Accepted
- **Context**: Need lightweight production images without build tooling bloat
- **Decision**: Use a multi-stage Docker build separating builder and runtime environments
- **Consequences**:
  - + Smaller final image size
  - + Improved security surface area
  - - Slightly more complex Dockerfile
- **Related Decisions**: ADR-021 (uv)

---

## ADR-023: Non-Root User in Docker for Security
- **Status**: Accepted
- **Context**: Running containers as root poses security risks
- **Decision**: Create and run container processes as non-root user `tcrm`
- **Consequences**:
  - + Adheres to container security best practices
  - + Prevents host privilege escalation vulnerabilities
  - - Requires careful file permission setup for config/data dirs
- **Related Decisions**: ADR-005 (Config paths)

---

## ADR-024: Host Network Mode for SF CLI Localhost Callbacks
- **Status**: Accepted
- **Context**: SF CLI web login flows require callback on `http://localhost:*`
- **Decision**: Use `network_mode: "host"` in Docker configuration for development/runtime containers
- **Consequences**:
  - + Seamless SF CLI web login callbacks inside Docker
  - + No port mapping conflicts
  - - Docker networking isolation bypassed for host interface
- **Related Decisions**: ADR-003 (SF CLI Auth)

---

## ADR-025: `pathlib.Path` and Platform Utils for Cross-Platform Support
- **Status**: Accepted
- **Context**: Codebase must run seamlessly on Windows, Linux, and macOS
- **Decision**: Use `pathlib.Path` exclusively and a centralized `platform.py` utility for OS-specific paths (`~/.config`, `AppData`, etc.)
- **Consequences**:
  - + Native OS path compatibility without manual string manipulation
  - + Standardized configuration and data directories per OS standards
  - - Requires discipline across all modules
- **Related Decisions**: ADR-005 (JSON File Persistence)

---

*Last updated: 2026-07-23*
*This document is append-only - never modify or delete existing entries.*