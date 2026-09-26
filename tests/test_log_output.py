from __future__ import annotations

import io
import re

from core.log_output import TimestampedRedactingStream


def test_stream_timestamps_lines_and_redacts_url_credentials():
    output = io.StringIO()
    stream = TimestampedRedactingStream(output)

    stream.write(
        "request failed: https://api.example.com/path?key=api-key-value&token=secret-token\n"
        "auth failed: https://user:password-value@example.com/path\n"
        "Traceback follows\n"
    )
    stream.flush()

    lines = output.getvalue().splitlines()
    timestamp_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    assert all(re.match(timestamp_pattern, line) for line in lines)
    assert "key=[REDACTED]&token=[REDACTED]" in lines[0]
    assert "api-key-value" not in output.getvalue()
    assert "secret-token" not in output.getvalue()
    assert "user:[REDACTED]@example.com" in lines[1]
    assert "password-value" not in output.getvalue()


def test_stream_redacts_assignment_and_bearer_credentials():
    output = io.StringIO()
    stream = TimestampedRedactingStream(output)

    stream.write("TRELLO_API_TOKEN=trello-secret Password: hidden Bearer abc.def\n")
    stream.flush()

    logged_text = output.getvalue()
    assert "trello-secret" not in logged_text
    assert "hidden" not in logged_text
    assert "abc.def" not in logged_text
    assert logged_text.count("[REDACTED]") == 3
