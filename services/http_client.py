"""Shared HTTP client with retry/backoff behavior."""

from __future__ import annotations

import sys
import time as time_module

import requests

from core.errors import SyncError


class HttpClient:
    """Issue HTTP requests with retry/backoff for transient failures."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_retry_delay_seconds: int = 2,
        retryable_status_codes: set[int] | None = None,
    ) -> None:
        """Initialize retry policy settings for outbound HTTP requests."""
        self.max_attempts = max_attempts
        self.initial_retry_delay_seconds = initial_retry_delay_seconds
        self.retryable_status_codes = retryable_status_codes or {429, 500, 502, 503, 504}

    def get_retry_delay_seconds(self, attempt_number: int) -> int:
        """Compute exponential backoff delay for a retry attempt."""
        return self.initial_retry_delay_seconds * (2 ** (attempt_number - 1))

    def is_retryable_http_error(self, error: requests.HTTPError) -> bool:
        """Return True when the HTTP status should be retried."""
        response = error.response
        return response is not None and response.status_code in self.retryable_status_codes

    def is_retryable_request_error(self, error: Exception) -> bool:
        """Return True for transient network errors and retryable HTTP errors."""
        if isinstance(error, (requests.ConnectionError, requests.Timeout)):
            return True
        if isinstance(error, requests.HTTPError):
            return self.is_retryable_http_error(error)
        return False

    def log_retry_attempt(self, message: str, attempt_number: int) -> None:
        """Log retry details and sleep using exponential backoff."""
        delay_seconds = self.get_retry_delay_seconds(attempt_number)
        print(
            f"{message}. Retrying in {delay_seconds} second(s) "
            f"(attempt {attempt_number + 1}/{self.max_attempts}).",
            file=sys.stderr,
        )
        time_module.sleep(delay_seconds)

    def request_with_backoff(
        self,
        method: str,
        url: str,
        retry_enabled: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Issue an HTTP request with retry and backoff."""
        for attempt_number in range(1, self.max_attempts + 1):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                if (
                    retry_enabled
                    and response.status_code in self.retryable_status_codes
                    and attempt_number < self.max_attempts
                ):
                    self.log_retry_attempt(
                        f"Received retryable HTTP status {response.status_code} for {method} {url}",
                        attempt_number,
                    )
                    continue
                return response
            except (requests.ConnectionError, requests.Timeout) as error:
                if not retry_enabled or attempt_number == self.max_attempts:
                    raise error
                self.log_retry_attempt(
                    f"Transient request failure for {method} {url}: {error}",
                    attempt_number,
                )

        raise SyncError(f"Request retries exhausted for {method} {url}")
