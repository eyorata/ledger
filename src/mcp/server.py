"""
src/mcp/server.py
MCP server entry point wrapper.
"""
from ledger.mcp_server import _get_store

try:
    from fastmcp import FastMCP
except Exception:  # pragma: no cover - optional dependency
    FastMCP = None

from src.mcp.tools import register_tools
from src.mcp.resources import register_resources


def create_mcp_server():
    if FastMCP is None:
        raise RuntimeError("fastmcp is not installed. Add fastmcp to your environment.")
    mcp = FastMCP("ledger-mcp")
    register_tools(mcp, _get_store)
    register_resources(mcp, _get_store)
    return mcp

__all__ = ["create_mcp_server"]
