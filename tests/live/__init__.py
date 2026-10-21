"""Opt-in contract tests that run against a real Salesforce org.

These are excluded from the default test run. Run them with:

    pytest -m live

They need ``SF_AUTH_TOKEN`` and ``SF_DEFAULT_DOMAIN`` in ``.env`` (or in
the environment).
"""
