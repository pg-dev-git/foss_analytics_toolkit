"""Tests for parsing `sf org list --json --all`.

The SF CLI does not return a single `orgs` array. It partitions authorized
orgs across `other`, `nonScratchOrgs`, `devHubs`, `scratchOrgs` and
sometimes `sandboxes`, and the same org routinely appears in more than one
of those arrays. Reading a non-existent `orgs` key made `auth list-orgs`
report "No authorized orgs found" on a machine with two authorized orgs.

The payload below is the real response shape captured from the SF CLI, with
the org-specific values replaced by placeholders.
"""

import json

import pytest

from asftool.core.sf_cli import SFCLIError, parse_org_list

# A verbatim-shaped `sf org list --json --all` response: the same org
# appears under both `other` and `nonScratchOrgs`, and the other arrays are
# present but empty.
SF_CLI_ORG_LIST = {
    "status": 0,
    "result": {
        "other": [
            {
                "accessToken": "[REDACTED]",
                "alias": "example-org",
                "connectedStatus": "Connected",
                "instanceApiVersion": "68.0",
                "instanceName": "example-org.my.salesforce.com",
                "instanceUrl": "https://example-org.my.salesforce.com/",
                "isDevHub": False,
                "isSandbox": False,
                "isScratch": False,
                "loginUrl": "https://example-org.my.salesforce.com/",
                "name": "Example User",
                "orgEdition": "Developer",
                "orgId": "00Dak0000000000AAA",
                "username": "user@example.com",
            }
        ],
        "sandboxes": [],
        "nonScratchOrgs": [
            {
                "accessToken": "[REDACTED]",
                "alias": "example-org",
                "connectedStatus": "Connected",
                "instanceApiVersion": "68.0",
                "instanceName": "example-org.my.salesforce.com",
                "instanceUrl": "https://example-org.my.salesforce.com/",
                "isDevHub": False,
                "isSandbox": False,
                "isScratch": False,
                "loginUrl": "https://example-org.my.salesforce.com/",
                "name": "Example User",
                "orgEdition": "Developer",
                "orgId": "00Dak0000000000AAA",
                "username": "user@example.com",
            }
        ],
        "devHubs": [],
        "scratchOrgs": [],
    },
}


class TestParseOrgList:
    def test_finds_orgs_in_the_real_response_shape(self):
        """The `orgs` key does not exist; the orgs live in sibling arrays."""
        orgs = parse_org_list(SF_CLI_ORG_LIST)
        assert len(orgs) == 1
        assert orgs[0]["alias"] == "example-org"

    def test_deduplicates_orgs_present_in_several_arrays(self):
        """The same org is listed under both `other` and `nonScratchOrgs`."""
        orgs = parse_org_list(SF_CLI_ORG_LIST)
        aliases = [org["alias"] for org in orgs]
        assert aliases == ["example-org"]

    def test_accepts_a_json_string(self):
        orgs = parse_org_list(json.dumps(SF_CLI_ORG_LIST))
        assert len(orgs) == 1

    def test_accepts_a_response_without_the_result_wrapper(self):
        orgs = parse_org_list(SF_CLI_ORG_LIST["result"])
        assert len(orgs) == 1

    def test_collects_orgs_from_every_array(self):
        payload = {
            "status": 0,
            "result": {
                "other": [{"alias": "a", "orgId": "00D1"}],
                "nonScratchOrgs": [{"alias": "b", "orgId": "00D2"}],
                "devHubs": [{"alias": "c", "orgId": "00D3"}],
                "scratchOrgs": [{"alias": "d", "orgId": "00D4"}],
                "sandboxes": [{"alias": "e", "orgId": "00D5"}],
            },
        }
        orgs = parse_org_list(payload)
        assert [org["alias"] for org in orgs] == ["a", "b", "c", "d", "e"]

    def test_empty_org_lists_return_empty(self):
        payload = {
            "status": 0,
            "result": {
                "other": [],
                "nonScratchOrgs": [],
                "devHubs": [],
                "scratchOrgs": [],
            },
        }
        assert parse_org_list(payload) == []

    def test_raises_on_unexpected_shape(self):
        """A parse failure must not look like 'you have no orgs'."""
        with pytest.raises(SFCLIError) as exc_info:
            parse_org_list({"status": 0, "result": {"somethingElse": []}})
        assert "somethingElse" in str(exc_info.value)

    def test_raises_on_invalid_json(self):
        with pytest.raises(SFCLIError):
            parse_org_list("not json at all")

    def test_raises_on_non_object_payload(self):
        with pytest.raises(SFCLIError):
            parse_org_list("[1, 2, 3]")
