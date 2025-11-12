#!/usr/bin/env python3
"""
Thin shim that re-uses the existing picks_cbb.py logic while exposing
build_picks() and JSON stdout, so deployment scripts can call this file
without touching the original implementation details.
"""

from picks_cbb import build_picks as _build_picks, main as _main  # noqa: F401


def build_picks():
    """Proxy to the real implementation in picks_cbb.py."""
    return _build_picks()


if __name__ == "__main__":
    import json

    picks = build_picks()
    print(json.dumps(picks))
