"""Git authentication models for multi-provider credential management.

Supports GitHub, GitLab, Bitbucket, and Azure DevOps with fine-grained PATs,
project tokens, or SSH keys.
"""

from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, SecretStr


class GitProvider(str, Enum):
    """Supported Git hosting providers."""

    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    AZURE_DEVOPS = "azure_devops"


class GitAuthType(str, Enum):
    """Authentication method types."""

    PAT = "pat"  # Personal Access Token (fine-grained preferred)
    PROJECT_TOKEN = "project_token"  # Project-scoped token (GitLab, Azure DevOps)
    SSH_KEY = "ssh_key"  # SSH private key
    USERNAME_PASSWORD = "username_password"  # Basic auth (legacy)


class SSHKeyFormat(str, Enum):
    """SSH key format types."""

    OPENSSH = "openssh"  # -----BEGIN OPENSSH PRIVATE KEY-----
    PKCS8 = "pkcs8"  # -----BEGIN PRIVATE KEY-----
    RSA = "rsa"  # -----BEGIN RSA PRIVATE KEY-----
    ED25519 = "ed25519"  # -----BEGIN OPENSSH PRIVATE KEY----- (Ed25519)


class GitCredentials(BaseModel):
    """Multi-provider Git credentials with encrypted secret storage.

    Supports:
    - GitHub: Fine-grained PAT (classic PAT also works)
    - GitLab: Personal Access Token or Project Access Token
    - Bitbucket: App Password or OAuth token
    - Azure DevOps: Personal Access Token (PAT)

    Environment variable fallbacks:
    - ASFTOOL_GIT_TOKEN: Generic token override
    - ASFTOOL_GIT_USERNAME: Username override
    - ASFTOOL_GIT_SSH_KEY: SSH private key override
    """

    model_config = {"frozen": True, "extra": "forbid"}

    # Provider identification
    provider: GitProvider = Field(..., description="Git hosting provider")

    # Repository identification (required for project-scoped tokens)
    host: str = Field(
        ...,
        description="Git host URL (e.g., github.com, gitlab.com, dev.azure.com)",
    )
    organization: Optional[str] = Field(
        None,
        description="Organization/namespace (GitHub org, GitLab group, Azure DevOps org)",
    )
    project: Optional[str] = Field(
        None,
        description="Project name (required for Azure DevOps, GitLab project tokens)",
    )

    # Authentication method
    auth_type: GitAuthType = Field(
        default=GitAuthType.PAT,
        description="Authentication method",
    )

    # Credentials (encrypted at rest)
    username: Optional[str] = Field(
        None,
        description="Username (for PAT: token owner; for SSH: git user, usually 'git')",
    )
    token: Optional[SecretStr] = Field(
        None,
        description="Personal Access Token or App Password (encrypted)",
    )
    ssh_private_key: Optional[SecretStr] = Field(
        None,
        description="SSH private key in PEM/OpenSSH format (encrypted)",
    )
    ssh_key_format: SSHKeyFormat = Field(
        default=SSHKeyFormat.OPENSSH,
        description="SSH private key format",
    )
    ssh_passphrase: Optional[SecretStr] = Field(
        None,
        description="SSH key passphrase if encrypted (encrypted)",
    )

    # Metadata
    alias: str = Field(
        ...,
        description="Human-readable alias for this credential set",
    )
    description: Optional[str] = Field(
        None,
        description="Optional description of what this credential accesses",
    )
    scopes: list[str] = Field(
        default_factory=list,
        description="Token scopes/permissions (for validation/auditing)",
    )

    # Environment variable fallbacks (not stored, used at runtime)
    _env_token: Optional[str] = Field(default=None, exclude=True)
    _env_username: Optional[str] = Field(default=None, exclude=True)
    _env_ssh_key: Optional[str] = Field(default=None, exclude=True)

    def get_effective_token(self) -> Optional[str]:
        """Get token with environment variable fallback.

        Priority: env var (ASFTOOL_GIT_TOKEN) > stored token
        """
        import os

        if self._env_token is not None:
            return self._env_token
        return os.environ.get("ASFTOOL_GIT_TOKEN") or (
            self.token.get_secret_value() if self.token else None
        )

    def get_effective_username(self) -> Optional[str]:
        """Get username with environment variable fallback.

        Priority: env var (ASFTOOL_GIT_USERNAME) > stored username
        """
        import os

        if self._env_username is not None:
            return self._env_username
        return os.environ.get("ASFTOOL_GIT_USERNAME") or self.username

    def get_effective_ssh_key(self) -> Optional[str]:
        """Get SSH key with environment variable fallback.

        Priority: env var (ASFTOOL_GIT_SSH_KEY) > stored key
        """
        import os

        if self._env_ssh_key is not None:
            return self._env_ssh_key
        return os.environ.get("ASFTOOL_GIT_SSH_KEY") or (
            self.ssh_private_key.get_secret_value() if self.ssh_private_key else None
        )

    def to_connection_config(self) -> dict:
        """Generate connection config for Git client libraries.

        Returns dict suitable for GitPython, dulwich, or httpx auth.
        """
        config = {
            "provider": self.provider.value,
            "host": self.host,
            "organization": self.organization,
            "project": self.project,
        }

        if self.auth_type == GitAuthType.SSH_KEY:
            ssh_key = self.get_effective_ssh_key()
            if ssh_key:
                config["ssh_key"] = ssh_key
                config["ssh_key_format"] = self.ssh_key_format.value
                if self.ssh_passphrase:
                    config["ssh_passphrase"] = self.ssh_passphrase.get_secret_value()
        else:
            token = self.get_effective_token()
            username = self.get_effective_username()
            if token:
                config["token"] = token
            if username:
                config["username"] = username

        return config

    @classmethod
    def from_connection_config(
        cls,
        config: dict,
        alias: str,
        provider: GitProvider,
        auth_type: GitAuthType = GitAuthType.PAT,
    ) -> "GitCredentials":
        """Create credentials from connection config dict."""
        return cls(
            provider=provider,
            host=config.get("host", ""),
            organization=config.get("organization"),
            project=config.get("project"),
            auth_type=auth_type,
            username=config.get("username"),
            token=SecretStr(config["token"]) if config.get("token") else None,
            ssh_private_key=(
                SecretStr(config["ssh_key"]) if config.get("ssh_key") else None
            ),
            ssh_key_format=SSHKeyFormat(config.get("ssh_key_format", "openssh")),
            ssh_passphrase=(
                SecretStr(config["ssh_passphrase"])
                if config.get("ssh_passphrase")
                else None
            ),
            alias=alias,
        )


class GitCredentialSummary(BaseModel):
    """Lightweight summary for listing credentials (no secrets)."""

    alias: str
    provider: GitProvider
    host: str
    organization: Optional[str] = None
    project: Optional[str] = None
    auth_type: GitAuthType
    description: Optional[str] = None
    scopes: list[str] = Field(default_factory=list)
    has_token: bool = False
    has_ssh_key: bool = False

    @classmethod
    def from_credentials(cls, creds: GitCredentials) -> "GitCredentialSummary":
        """Create summary from full credentials."""
        return cls(
            alias=creds.alias,
            provider=creds.provider,
            host=creds.host,
            organization=creds.organization,
            project=creds.project,
            auth_type=creds.auth_type,
            description=creds.description,
            scopes=creds.scopes,
            has_token=creds.token is not None,
            has_ssh_key=creds.ssh_private_key is not None,
        )


class GitRepositoryTarget(BaseModel):
    """Target repository configuration for asset routing."""

    provider: GitProvider
    host: str
    organization: str
    repository: str
    project: Optional[str] = None  # For Azure DevOps / GitLab
    branch: str = "main"
    credentials_alias: str  # References GitCredentials.alias
    path_prefix: str = ""  # Subdirectory within repo

    @property
    def clone_url_https(self) -> str:
        """Generate HTTPS clone URL."""
        base = f"https://{self.host}"
        if self.provider == GitProvider.AZURE_DEVOPS:
            if self.project:
                return f"{base}/{self.organization}/{self.project}/_git/{self.repository}"
            return f"{base}/{self.organization}/_git/{self.repository}"
        elif self.provider == GitProvider.GITLAB:
            if self.project:
                return f"{base}/{self.organization}/{self.project}.git"
            return f"{base}/{self.organization}/{self.repository}.git"
        else:  # GitHub, Bitbucket
            return f"{base}/{self.organization}/{self.repository}.git"

    @property
    def clone_url_ssh(self) -> str:
        """Generate SSH clone URL."""
        if self.provider == GitProvider.AZURE_DEVOPS:
            if self.project:
                return f"ssh://{self.host}/{self.organization}/{self.project}/_git/{self.repository}"
            return f"ssh://{self.host}/{self.organization}/_git/{self.repository}"
        elif self.provider == GitProvider.GITLAB:
            return f"git@{self.host}:{self.organization}/{self.repository}.git"
        else:  # GitHub, Bitbucket
            return f"git@{self.host}:{self.organization}/{self.repository}.git"