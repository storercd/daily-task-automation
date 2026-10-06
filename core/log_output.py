"""Timestamp and redact text written by the command-line application."""

from __future__ import annotations

import re
import sys
from datetime import datetime
from typing import TextIO

_QUERY_SECRET_PATTERN = re.compile(
    r"([?&](?:key|token|password|secret|client_secret|access_token|refresh_token)=)[^&#\s\"'<>)]*",
    re.IGNORECASE,
)
_URL_PASSWORD_PATTERN = re.compile(r"(https?://[^:/\s@]+:)[^@/\s]+(@)", re.IGNORECASE)
_ASSIGNMENT_SECRET_PATTERN = re.compile(
    r"(\b[A-Z0-9_]*(?:API[_-]?KEY|API[_-]?TOKEN|ACCESS[_-]?TOKEN|REFRESH[_-]?TOKEN|CLIENT[_-]?SECRET|TOKEN|PASSWORD|SECRET)\b\s*[:=]\s*[\"']?)([^&\s,;}'\"]+)",
    re.IGNORECASE,
)
_BEARER_SECRET_PATTERN = re.compile(r"(\bBearer\s+)[A-Za-z0-9._~+/-]+=*", re.IGNORECASE)


def redact_sensitive_values(text: str) -> str:
    """Mask common credentials in URLs, assignments, and bearer headers."""
    text = _QUERY_SECRET_PATTERN.sub(r"\1[REDACTED]", text)
    text = _URL_PASSWORD_PATTERN.sub(r"\1[REDACTED]\2", text)
    text = _ASSIGNMENT_SECRET_PATTERN.sub(r"\1[REDACTED]", text)
    return _BEARER_SECRET_PATTERN.sub(r"\1[REDACTED]", text)


class TimestampedRedactingStream:
    """Proxy a text stream, adding a local timestamp and masking credentials per line."""

    def __init__(self, stream: TextIO) -> None:
        """Wrap the given text stream to buffer and process writes line by line."""
        self.stream = stream
        self.pending_text = ""

    def write(self, text: str) -> int:
        """Buffer written text and flush any complete lines to the wrapped stream."""
        self.pending_text += text
        while "\n" in self.pending_text:
            line, self.pending_text = self.pending_text.split("\n", 1)
            self._write_line(line)
        return len(text)

    def flush(self) -> None:
        """Write any buffered partial line and flush the wrapped stream."""
        if self.pending_text:
            self._write_line(self.pending_text)
            self.pending_text = ""
        self.stream.flush()

    def _write_line(self, line: str) -> None:
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        self.stream.write(f"{timestamp} {redact_sensitive_values(line)}\n")
        self.stream.flush()

    def __getattr__(self, name: str):
        """Delegate attribute access not handled above to the wrapped stream."""
        return getattr(self.stream, name)


def install_log_output_filters() -> None:
    """Timestamp and redact the CLI's stdout and stderr output."""
    if not isinstance(sys.stdout, TimestampedRedactingStream):
        sys.stdout = TimestampedRedactingStream(sys.stdout)
    if not isinstance(sys.stderr, TimestampedRedactingStream):
        sys.stderr = TimestampedRedactingStream(sys.stderr)
