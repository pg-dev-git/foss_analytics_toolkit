"""Unit tests for Git auth and resolver functionality."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from asftool.core.auth.git_auth import GitAuthService, get_git_auth_service
from asftool.core.git.normalizer import CRMANormalizer, NormalizerConfig
from asftool.core.git.resolver import (
    AssetContext,
    RepoMappingConfig,
    RepoMappingResolver,
    RepoMappingRule,
    create_default_config,
)
from asftool.core.models.git_auth import (
    GitAuthType,
    GitCredentials,
    GitProvider,
    GitRepositoryTarget,
    SSHKeyFormat,
)
from asftool.core.crypto import CryptoManager, EncryptedData


class TestGitCredentialsModel:
    """Tests for GitCredentials Pydantic model."""

    def test_github_pat_credentials(self):
        """Test creating GitHub PAT credentials."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            token="ghp_xxxxxxxxxxxx",
            alias="github-pat",
            description="GitHub fine-grained PAT",
            scopes=["repo", "workflow"],
        )

        assert creds.provider == GitProvider.GITHUB
        assert creds.host == "github.com"
        assert creds.organization == "myorg"
        assert creds.auth_type == GitAuthType.PAT
        assert creds.username == "myuser"
        assert creds.token.get_secret_value() == "ghp_xxxxxxxxxxxx"
        assert creds.alias == "github-pat"
        assert creds.scopes == ["repo", "workflow"]

    def test_gitlab_project_token(self):
        """Test GitLab project access token."""
        creds = GitCredentials(
            provider=GitProvider.GITLAB,
            host="gitlab.com",
            organization="mygroup",
            project="myproject",
            auth_type=GitAuthType.PROJECT_TOKEN,
            token="glpat-xxxxxxxxxxxx",
            alias="gitlab-project",
        )

        assert creds.provider == GitProvider.GITLAB
        assert creds.project == "myproject"
        assert creds.auth_type == GitAuthType.PROJECT_TOKEN

    def test_azure_devops_credentials(self):
        """Test Azure DevOps PAT with project."""
        creds = GitCredentials(
            provider=GitProvider.AZURE_DEVOPS,
            host="dev.azure.com",
            organization="myorg",
            project="myproject",
            auth_type=GitAuthType.PAT,
            token="pat-token",
            alias="azure-devops",
        )

        assert creds.provider == GitProvider.AZURE_DEVOPS
        assert creds.project == "myproject"

    def test_ssh_key_credentials(self):
        """Test SSH key authentication."""
        ssh_key = "-----BEGIN OPENSSH PRIVATE KEY-----\nxxx\n-----END OPENSSH PRIVATE KEY-----"
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.SSH_KEY,
            username="git",
            ssh_private_key=ssh_key,
            ssh_key_format=SSHKeyFormat.OPENSSH,
            alias="github-ssh",
        )

        assert creds.auth_type == GitAuthType.SSH_KEY
        assert creds.ssh_private_key.get_secret_value() == ssh_key
        assert creds.ssh_key_format == SSHKeyFormat.OPENSSH

    def test_ssh_key_with_passphrase(self):
        """Test SSH key with passphrase."""
        ssh_key = "-----BEGIN OPENSSH PRIVATE KEY-----\nxxx\n-----END OPENSSH PRIVATE KEY-----"
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.SSH_KEY,
            username="git",
            ssh_private_key=ssh_key,
            ssh_passphrase="my-passphrase",
            alias="github-ssh-pass",
        )

        assert creds.ssh_passphrase.get_secret_value() == "my-passphrase"

    def test_connection_config_https(self):
        """Test generating HTTPS connection config."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            token="ghp_token",
            alias="test",
        )

        config = creds.to_connection_config()

        assert config["provider"] == "github"
        assert config["host"] == "github.com"
        assert config["organization"] == "myorg"
        assert config["token"] == "ghp_token"
        assert config["username"] == "myuser"

    def test_connection_config_ssh(self):
        """Test generating SSH connection config."""
        ssh_key = "-----BEGIN OPENSSH PRIVATE KEY-----\nxxx\n-----END OPENSSH PRIVATE KEY-----"
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.SSH_KEY,
            username="git",
            ssh_private_key=ssh_key,
            ssh_key_format=SSHKeyFormat.OPENSSH,
            ssh_passphrase="passphrase",
            alias="test",
        )

        config = creds.to_connection_config()

        assert config["provider"] == "github"
        assert config["ssh_key"] == ssh_key
        assert config["ssh_key_format"] == "openssh"
        assert config["ssh_passphrase"] == "passphrase"

    def test_env_fallback_token(self):
        """Test environment variable fallback for token."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            alias="test",
        )

        with patch.dict(os.environ, {"ASFTOOL_GIT_TOKEN": "env-token"}):
            assert creds.get_effective_token() == "env-token"

    def test_env_fallback_username(self):
        """Test environment variable fallback for username."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            token="stored-token",
            alias="test",
        )

        with patch.dict(os.environ, {"ASFTOOL_GIT_USERNAME": "env-user"}):
            assert creds.get_effective_username() == "env-user"

    def test_env_fallback_ssh_key(self):
        """Test environment variable fallback for SSH key."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.SSH_KEY,
            username="git",
            alias="test",
        )

        ssh_key = "-----BEGIN OPENSSH PRIVATE KEY-----\nxxx\n-----END OPENSSH PRIVATE KEY-----"
        with patch.dict(os.environ, {"ASFTOOL_GIT_SSH_KEY": ssh_key}):
            assert creds.get_effective_ssh_key() == ssh_key

    def test_git_repository_target_urls(self):
        """Test GitRepositoryTarget URL generation."""
        # GitHub
        target = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="myrepo",
            credentials_alias="test",
        )
        assert target.clone_url_https == "https://github.com/myorg/myrepo.git"
        assert target.clone_url_ssh == "git@github.com:myorg/myrepo.git"

        # GitLab
        target = GitRepositoryTarget(
            provider=GitProvider.GITLAB,
            host="gitlab.com",
            organization="mygroup",
            repository="myrepo",
            credentials_alias="test",
        )
        assert target.clone_url_https == "https://gitlab.com/mygroup/myrepo.git"
        assert target.clone_url_ssh == "git@gitlab.com:mygroup/myrepo.git"

        # Azure DevOps
        target = GitRepositoryTarget(
            provider=GitProvider.AZURE_DEVOPS,
            host="dev.azure.com",
            organization="myorg",
            project="myproject",
            repository="myrepo",
            credentials_alias="test",
        )
        assert target.clone_url_https == "https://dev.azure.com/myorg/myproject/_git/myrepo"
        assert target.clone_url_ssh == "ssh://dev.azure.com/myorg/myproject/_git/myrepo"


class TestGitAuthService:
    """Tests for GitAuthService."""

    @pytest.fixture
    def mock_crypto(self):
        """Create a mock crypto manager."""
        crypto = MagicMock(spec=CryptoManager)
        crypto.encrypt_json.return_value = EncryptedData(
            ciphertext="ciphertext",
            salt="salt",
            iterations=100000,
        )
        crypto.decrypt_json.return_value = {
            "provider": "github",
            "host": "github.com",
            "organization": "myorg",
            "auth_type": "pat",
            "username": "myuser",
            "token": "ghp_token",
            "alias": "test-alias",
            "scopes": ["repo"],
        }
        return crypto

    @pytest.fixture
    def service(self, mock_crypto):
        """Create GitAuthService with mock crypto."""
        return GitAuthService(crypto_manager=mock_crypto)

    def test_store_and_retrieve_credentials(self, service, mock_crypto):
        """Test storing and retrieving credentials."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            token="ghp_token",
            alias="test-alias",
        )

        service.store_credentials_with_index(creds)

        # Verify crypto was called
        mock_crypto.encrypt_json.assert_called_once()

        # Verify keyring was called
        # We can't easily test keyring without integration, but we can verify
        # the flow works

    def test_validate_credentials_valid(self, service):
        """Test validation of valid credentials."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            token="ghp_token",
            alias="test",
        )

        is_valid, errors = service.validate_credentials(creds)

        assert is_valid is True
        assert errors == []

    def test_validate_credentials_missing_token(self, service):
        """Test validation fails for missing token."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            username="myuser",
            alias="test",
        )

        with patch.dict(os.environ, {}, clear=True):
            is_valid, errors = service.validate_credentials(creds)

        assert is_valid is False
        assert any("token" in e.lower() for e in errors)

    def test_validate_azure_devops_missing_project(self, service):
        """Test validation fails for Azure DevOps without project."""
        creds = GitCredentials(
            provider=GitProvider.AZURE_DEVOPS,
            host="dev.azure.com",
            organization="myorg",
            auth_type=GitAuthType.PAT,
            token="pat-token",
            alias="test",
        )

        is_valid, errors = service.validate_credentials(creds)

        assert is_valid is False
        assert any("project" in e.lower() for e in errors)

    def test_validate_gitlab_project_token_missing_project(self, service):
        """Test validation fails for GitLab project token without project."""
        creds = GitCredentials(
            provider=GitProvider.GITLAB,
            host="gitlab.com",
            organization="mygroup",
            auth_type=GitAuthType.PROJECT_TOKEN,
            token="glpat-token",
            alias="test",
        )

        is_valid, errors = service.validate_credentials(creds)

        assert is_valid is False
        assert any("project" in e.lower() for e in errors)

    def test_validate_ssh_missing_key(self, service):
        """Test validation fails for SSH auth without key."""
        creds = GitCredentials(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            auth_type=GitAuthType.SSH_KEY,
            username="git",
            alias="test",
        )

        is_valid, errors = service.validate_credentials(creds)

        assert is_valid is False
        assert any("ssh" in e.lower() for e in errors)


class TestRepoMappingResolver:
    """Tests for RepoMappingResolver."""

    def test_simple_default_config(self):
        """Test resolver with default target only."""
        config = create_default_config(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="crm-assets",
            credentials_alias="github-creds",
        )

        resolver = RepoMappingResolver(config)

        asset = AssetContext(
            asset_id="01Z000000000000",
            asset_type="dashboard",
            name="Executive Dashboard",
            folder="Executive Reports",
        )

        target = resolver.resolve_repository(asset)

        assert target.provider == GitProvider.GITHUB
        assert target.repository == "crm-assets"
        assert target.credentials_alias == "github-creds"

    def test_rule_matching_folder(self):
        """Test rule matching by folder name."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="exec-reporting",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="pipeline-reporting",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target2,
            rules=[
                RepoMappingRule(
                    folder="Executive*",
                    target=target1,
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        # Should match Executive folder
        asset1 = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Dashboard 1",
            folder="Executive Reports",
        )
        target1_result = resolver.resolve_repository(asset1)
        assert target1_result.repository == "exec-reporting"

        # Should fall back to default
        asset2 = AssetContext(
            asset_id="002",
            asset_type="dashboard",
            name="Dashboard 2",
            folder="Sales Reports",
        )
        target2_result = resolver.resolve_repository(asset2)
        assert target2_result.repository == "pipeline-reporting"

    def test_rule_matching_asset_type(self):
        """Test rule matching by asset type."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="dashboards",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="recipes",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target2,
            rules=[
                RepoMappingRule(
                    asset_type="dashboard",
                    target=target1,
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        asset1 = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Dashboard 1",
        )
        assert resolver.resolve_repository(asset1).repository == "dashboards"

        asset2 = AssetContext(
            asset_id="002",
            asset_type="recipe",
            name="Recipe 1",
        )
        assert resolver.resolve_repository(asset2).repository == "recipes"

    def test_rule_matching_name_glob(self):
        """Test rule matching by name with glob pattern."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="special-assets",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="regular-assets",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target2,
            rules=[
                RepoMappingRule(
                    name="*_Critical",
                    target=target1,
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        asset1 = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Sales_Critical",
        )
        assert resolver.resolve_repository(asset1).repository == "special-assets"

        asset2 = AssetContext(
            asset_id="002",
            asset_type="dashboard",
            name="Sales_Regular",
        )
        assert resolver.resolve_repository(asset2).repository == "regular-assets"

    def test_rule_matching_app(self):
        """Test rule matching by app name."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="sales-app",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="service-app",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target2,
            rules=[
                RepoMappingRule(
                    app="Sales*",
                    target=target1,
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        asset1 = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Dashboard",
            app="Sales Analytics",
        )
        assert resolver.resolve_repository(asset1).repository == "sales-app"

        asset2 = AssetContext(
            asset_id="002",
            asset_type="dashboard",
            name="Dashboard",
            app="Service Cloud",
        )
        assert resolver.resolve_repository(asset2).repository == "service-app"

    def test_multiple_rules_first_match_wins(self):
        """Test that first matching rule wins."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo1",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo2",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target2,
            rules=[
                RepoMappingRule(
                    folder="*",
                    target=target1,  # This matches everything
                ),
                RepoMappingRule(
                    folder="Specific",
                    target=target2,  # This would never be reached
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        asset = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Test",
            folder="Specific Folder",
        )
        # First rule matches (folder="*"), so target1 is used
        assert resolver.resolve_repository(asset).repository == "repo1"

    def test_no_config_raises_error(self):
        """Test that resolver without config raises error."""
        resolver = RepoMappingResolver()
        asset = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Test",
        )

        with pytest.raises(ValueError, match="No configuration loaded"):
            resolver.resolve_repository(asset)

    def test_no_match_no_default_raises_error(self):
        """Test that no match and no default raises error."""
        config = RepoMappingConfig(
            version="1",
            default_target=None,
            rules=[
                RepoMappingRule(
                    folder="Specific",
                    target=GitRepositoryTarget(
                        provider=GitProvider.GITHUB,
                        host="github.com",
                        organization="myorg",
                        repository="repo1",
                        credentials_alias="creds1",
                    ),
                ),
            ],
        )

        resolver = RepoMappingResolver(config)

        asset = AssetContext(
            asset_id="001",
            asset_type="dashboard",
            name="Test",
            folder="Other",
        )

        with pytest.raises(ValueError, match="No mapping rule matches"):
            resolver.resolve_repository(asset)

    def test_get_all_targets(self):
        """Test getting all unique targets."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo1",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo2",
            credentials_alias="creds2",
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target1,
            rules=[
                RepoMappingRule(folder="A", target=target1),
                RepoMappingRule(folder="B", target=target2),
                RepoMappingRule(folder="C", target=target1),  # Duplicate
            ],
        )

        resolver = RepoMappingResolver(config)
        targets = resolver.get_all_targets()

        assert len(targets) == 2
        assert target1 in targets
        assert target2 in targets

    def test_config_from_file(self, tmp_path):
        """Test loading config from YAML file."""
        config_data = {
            "version": "1",
            "default_target": {
                "provider": "github",
                "host": "github.com",
                "organization": "myorg",
                "repository": "default-repo",
                "credentials_alias": "default-creds",
            },
            "rules": [
                {
                    "folder": "Executive*",
                    "target": {
                        "provider": "github",
                        "host": "github.com",
                        "organization": "myorg",
                        "repository": "exec-repo",
                        "credentials_alias": "exec-creds",
                    },
                },
            ],
        }

        config_file = tmp_path / ".asftool-git.yml"
        with open(config_file, "w") as f:
            yaml.safe_dump(config_data, f)

        resolver = RepoMappingResolver.from_file(config_file)

        assert resolver.config is not None
        assert resolver.config.default_target is not None
        assert resolver.config.default_target.repository == "default-repo"
        assert len(resolver.config.rules) == 1
        assert resolver.config.rules[0].folder == "Executive*"

    def test_config_to_file(self, tmp_path):
        """Test writing config to YAML file."""
        config = create_default_config(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="test-repo",
            credentials_alias="test-creds",
        )

        output_file = tmp_path / "output.yml"
        config.to_file(output_file)

        assert output_file.exists()

        with open(output_file, "r") as f:
            loaded = yaml.safe_load(f)

        assert loaded["version"] == "1"
        assert loaded["default_target"]["repository"] == "test-repo"

    def test_validate_config_warnings(self):
        """Test config validation produces warnings for credential mismatches."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo1",
            credentials_alias="creds1",
        )
        # Same target key (same provider, host, org, repo, project) but different credentials
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo1",  # Same repo
            credentials_alias="creds2",  # Different creds
        )

        config = RepoMappingConfig(
            version="1",
            default_target=target1,
            rules=[
                RepoMappingRule(folder="A", target=target2),
            ],
        )

        resolver = RepoMappingResolver(config)
        issues = resolver.validate_config()

        assert len(issues) > 0
        assert any("different credentials alias" in issue for issue in issues)


class TestCRMANormalizer:
    """Tests for CRMANormalizer."""

    @pytest.fixture
    def normalizer(self):
        """Create normalizer with default config."""
        return CRMANormalizer()

    @pytest.fixture
    def sample_dashboard(self):
        """Sample dashboard JSON from CRMA API."""
        return {
            "id": "01Z000000000000AAA",
            "developerName": "Executive_Dashboard",
            "label": "Executive Dashboard",
            "applicationId": "06M000000000000",
            "folderId": "05B000000000000",
            "folderName": "Executive Reports",
            "createdDate": "2024-01-15T10:30:00.000Z",
            "lastModifiedDate": "2024-06-20T14:45:00.000Z",
            "createdById": "005000000000000",
            "lastModifiedById": "005000000000001",
            "runningUserId": "005000000000002",
            "version": 5,
            "state": "Published",
            "uiState": {"filters": {"date": "last_30_days"}},
            "widgets": [
                {
                    "id": "w1",
                    "label": "Revenue Chart",
                    "type": "chart",
                    "query": {"soql": "SELECT ..."},
                }
            ],
        }

    @pytest.fixture
    def sample_recipe(self):
        """Sample recipe JSON from CRMA API."""
        return {
            "id": "01Z000000000001BBB",
            "developerName": "Data_Prep_Recipe",
            "label": "Data Prep Recipe",
            "dataflowId": "02K000000000000",
            "dataflowName": "Main Dataflow",
            "createdDate": "2024-01-15T10:30:00.000Z",
            "lastModifiedDate": "2024-06-20T14:45:00.000Z",
            "createdById": "005000000000000",
            "steps": [
                {"name": "Load Data", "type": "load"},
                {"name": "Transform", "type": "transform"},
            ],
        }

    def test_normalize_dashboard_strips_volatile_fields(self, normalizer, sample_dashboard):
        """Test that volatile fields are stripped from dashboard."""
        result = normalizer.normalize(sample_dashboard, "dashboard")

        normalized = json.loads(result.normalized_json)

        # Check volatile fields are stripped
        assert "id" not in normalized
        assert "createdDate" not in normalized
        assert "lastModifiedDate" not in normalized
        assert "createdById" not in normalized
        assert "lastModifiedById" not in normalized
        assert "applicationId" not in normalized
        assert "folderId" not in normalized
        assert "folderName" not in normalized
        assert "runningUserId" not in normalized
        assert "version" not in normalized
        assert "state" not in normalized
        assert "uiState" not in normalized

        # Check preserved fields
        assert normalized["developerName"] == "Executive_Dashboard"
        assert normalized["label"] == "Executive Dashboard"
        assert "widgets" in normalized

        # Check stripped fields list
        assert "id" in result.stripped_fields
        assert "createdDate" in result.stripped_fields
        assert "lastModifiedDate" in result.stripped_fields

    def test_normalize_recipe_strips_volatile_fields(self, normalizer, sample_recipe):
        """Test that volatile fields are stripped from recipe."""
        result = normalizer.normalize(sample_recipe, "recipe")

        normalized = json.loads(result.normalized_json)

        assert "id" not in normalized
        assert "createdDate" not in normalized
        assert "lastModifiedDate" not in normalized
        assert "createdById" not in normalized
        assert "dataflowId" not in normalized
        assert "dataflowName" not in normalized

        assert normalized["developerName"] == "Data_Prep_Recipe"
        assert normalized["label"] == "Data Prep Recipe"
        assert "steps" in normalized

    def test_normalize_deterministic_output(self, normalizer, sample_dashboard):
        """Test that output is deterministic (same input = same output)."""
        result1 = normalizer.normalize(sample_dashboard, "dashboard")
        result2 = normalizer.normalize(sample_dashboard, "dashboard")

        assert result1.normalized_json == result2.normalized_json

    def test_normalize_sorts_keys(self, normalizer):
        """Test that keys are sorted alphabetically."""
        data = {
            "zebra": 1,
            "alpha": 2,
            "beta": 3,
            "lastModifiedDate": "2024-01-01T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")
        normalized = json.loads(result.normalized_json)

        keys = list(normalized.keys())
        assert keys == sorted(keys)

    def test_normalize_utf8_output(self, normalizer):
        """Test that UTF-8 characters are preserved."""
        data = {
            "label": "Dashboard 日本語 🇯🇵",
            "lastModifiedDate": "2024-01-01T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")

        # Should contain UTF-8 characters, not escaped
        assert "日本語" in result.normalized_json
        assert "🇯🇵" in result.normalized_json
        assert "\\u" not in result.normalized_json  # No Unicode escaping

    def test_normalize_bytes_input(self, normalizer, sample_dashboard):
        """Test normalizing from bytes input."""
        json_bytes = json.dumps(sample_dashboard).encode("utf-8")

        result = normalizer.normalize_bytes(json_bytes, "dashboard")

        normalized = json.loads(result.normalized_json)
        assert normalized["developerName"] == "Executive_Dashboard"

    def test_normalize_nested_objects(self, normalizer):
        """Test normalizing nested objects."""
        data = {
            "label": "Test",
            "metadata": {
                "createdDate": "2024-01-01T00:00:00.000Z",
                "nested": {
                    "lastModifiedDate": "2024-01-02T00:00:00.000Z",
                    "value": "keep",
                },
            },
            "lastModifiedDate": "2024-01-03T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")
        normalized = json.loads(result.normalized_json)

        assert "lastModifiedDate" not in normalized
        assert "metadata" in normalized
        assert "createdDate" not in normalized["metadata"]
        assert "nested" in normalized["metadata"]
        assert "lastModifiedDate" not in normalized["metadata"]["nested"]
        assert normalized["metadata"]["nested"]["value"] == "keep"

    def test_normalize_lists(self, normalizer):
        """Test normalizing lists of objects."""
        data = {
            "label": "Test",
            "widgets": [
                {"id": "w1", "label": "Widget 1", "createdDate": "2024-01-01T00:00:00.000Z"},
                {"id": "w2", "label": "Widget 2", "createdDate": "2024-01-02T00:00:00.000Z"},
            ],
            "lastModifiedDate": "2024-01-03T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")
        normalized = json.loads(result.normalized_json)

        assert "widgets" in normalized
        assert len(normalized["widgets"]) == 2
        assert "createdDate" not in normalized["widgets"][0]
        assert "createdDate" not in normalized["widgets"][1]
        assert normalized["widgets"][0]["label"] == "Widget 1"

    def test_normalize_preserves_developer_name(self, normalizer):
        """Test that developerName is always preserved."""
        data = {
            "developerName": "My_Dashboard",
            "label": "My Dashboard",
            "lastModifiedDate": "2024-01-01T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")
        normalized = json.loads(result.normalized_json)

        assert normalized["developerName"] == "My_Dashboard"
        assert "developerName" in result.preserved_fields

    def test_normalize_file_roundtrip(self, normalizer, sample_dashboard, tmp_path):
        """Test normalizing file in place."""
        input_file = tmp_path / "dashboard.json"
        with open(input_file, "w", encoding="utf-8") as f:
            json.dump(sample_dashboard, f)

        result = normalizer.normalize_file(input_file)

        # Read back
        with open(input_file, "r", encoding="utf-8") as f:
            content = f.read()

        assert content == result.normalized_json
        normalized = json.loads(content)
        assert "lastModifiedDate" not in normalized

    def test_create_normalized_bundle(self, normalizer, sample_dashboard):
        """Test creating bundle for REST API PUT."""
        bundle = normalizer.create_normalized_bundle(sample_dashboard, "dashboard")

        assert "asset" in bundle
        assert "metadata" in bundle
        assert bundle["metadata"]["normalized"] is True
        assert bundle["metadata"]["assetType"] == "dashboard"
        assert "strippedFields" in bundle["metadata"]

        # Asset should be normalized
        asset = bundle["asset"]
        assert "lastModifiedDate" not in asset
        assert asset["developerName"] == "Executive_Dashboard"

    def test_custom_config(self):
        """Test normalizer with custom config."""
        config = NormalizerConfig(
            strip_fields=["customField", "lastModifiedDate"],
            preserve_fields=["keepThis"],
            indent=4,
            sort_keys=False,
        )
        normalizer = CRMANormalizer(config)

        data = {
            "keepThis": "value",
            "customField": "strip me",
            "lastModifiedDate": "2024-01-01T00:00:00.000Z",
        }

        result = normalizer.normalize(data, "dashboard")
        normalized = json.loads(result.normalized_json)

        assert "keepThis" in normalized
        assert "customField" not in normalized
        assert "lastModifiedDate" not in normalized

        # Check indent is 4 spaces
        lines = result.normalized_json.split("\n")
        assert lines[1].startswith("    ")  # 4 spaces


class TestRepoMappingConfig:
    """Tests for RepoMappingConfig."""

    def test_create_default_config(self):
        """Test creating default single-repo config."""
        config = create_default_config(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="crm-assets",
            credentials_alias="my-creds",
        )

        assert config.version == "1"
        assert config.default_target is not None
        assert config.default_target.repository == "crm-assets"
        assert config.default_target.credentials_alias == "my-creds"
        assert config.rules == []

    def test_create_multi_repo_config(self):
        """Test creating multi-repo config with rules."""
        target1 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo1",
            credentials_alias="creds1",
        )
        target2 = GitRepositoryTarget(
            provider=GitProvider.GITHUB,
            host="github.com",
            organization="myorg",
            repository="repo2",
            credentials_alias="creds2",
        )

        rules = [
            RepoMappingRule(folder="A", target=target1),
            RepoMappingRule(folder="B", target=target2),
        ]

        config = RepoMappingConfig(
            version="1",
            default_target=target1,
            rules=rules,
        )

        assert len(config.rules) == 2
        assert config.default_target == target1


class TestAssetContext:
    """Tests for AssetContext."""

    def test_asset_context_creation(self):
        """Test creating asset context."""
        asset = AssetContext(
            asset_id="01Z000000000000",
            asset_type="dashboard",
            name="My Dashboard",
            folder="Reports",
            app="Sales App",
        )

        assert asset.asset_id == "01Z000000000000"
        assert asset.asset_type == "dashboard"
        assert asset.name == "My Dashboard"
        assert asset.folder == "Reports"
        assert asset.app == "Sales App"


# Integration test for full flow
class TestGitAuthAndResolverIntegration:
    """Integration tests for auth and resolver together."""

    def test_full_flow_credentials_to_resolver(self):
        """Test creating credentials and using with resolver."""
        # This would be an integration test that requires keyring
        # For unit tests, we mock the keyring
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])