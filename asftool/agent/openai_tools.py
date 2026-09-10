"""OpenAI Function Calling schema exporter for CRMA SDK."""

SCHEMAS = {
    "crma_list_datasets": {
        "type": "function",
        "function": {
            "name": "crma_list_datasets",
            "description": "List Salesforce Analytics datasets",
            "parameters": {"type": "object", "properties": {"page_size": {"type": "integer"}}},
        },
    },
    "crma_query_saql": {
        "type": "function",
        "function": {
            "name": "crma_query_saql",
            "description": "Run SAQL query against dataset",
            "parameters": {"type": "object", "properties": {"query": {"type": "string"}}},
        },
    },
}


def export_openai_schemas() -> list[dict]:
    return list(SCHEMAS.values())
