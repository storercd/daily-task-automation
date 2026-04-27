"""Shared domain exceptions for task automation routines."""

from __future__ import annotations


class SyncError(Exception):
    """Raise when a domain-level synchronization step cannot be completed."""
