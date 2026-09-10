"""Offline respx mock Salesforce REST API server for E2E tests."""

MOCK_DATASETS = [
    {"id": "0Fb000000000001", "name": "TestDataset", "label": "Test Dataset"},
]

MOCK_SAQL_RESULTS = {
    "results": {"records": [{"cnt": 42}]}
}


def get_mock_dataset_list():
    return {"datasets": MOCK_DATASETS, "total": len(MOCK_DATASETS)}
