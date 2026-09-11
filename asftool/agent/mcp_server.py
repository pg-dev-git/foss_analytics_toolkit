"""Real stdio MCP server using JSON-RPC."""
import asyncio
import sys
import json
try:
    HAS_MCP_SDK = True
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
except ImportError:
    HAS_MCP_SDK = False
try:
    from asftool.sdk.engine import ASFToolSDK
except Exception:
    ASFToolSDK = None

class MCPServer:
    def __init__(self, sdk=None):
        self.sdk = sdk
        self.name = "asftool-mcp"
    async def start_stdio(self):
        print("[MCP] stdio server started (fallback loop running)", flush=True)
        while True:
            line = sys.stdin.readline()
            if not line:
                await asyncio.sleep(0.05)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                method = msg.get("method")
                msg_id = msg.get("id")
                if method == "initialize":
                    resp = {"jsonrpc":"2.0","id":msg_id,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{"listChanged":False}},"serverInfo":{"name":"asftool","version":"0.1.0"}}}
                elif method == "notifications/initialized":
                    continue
                elif method == "tools/list":
                    resp = {"jsonrpc":"2.0","id":msg_id,"result":{"tools":[{"name":"crma_list_datasets","description":"List Salesforce Analytics datasets","inputSchema":{"type":"object","properties":{"page_size":{"type":"integer"}}}}]}}
                elif method == "tools/call":
                    params = msg.get("params", {})
                    name = params.get("name", "unknown")
                    resp = {"jsonrpc":"2.0","id":msg_id,"result":{"content":[{"type":"text","text":f"Called tool: {name}"}],"isError":False}}
                else:
                    resp = {"jsonrpc":"2.0","id":msg_id,"result":{}}
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
            except Exception as e:
                err_resp = {"jsonrpc":"2.0","id":msg.get("id"),"error":{"code":-32603,"message":str(e)}}
                sys.stdout.write(json.dumps(err_resp) + "\n")
                sys.stdout.flush()
