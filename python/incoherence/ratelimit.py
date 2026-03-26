"""Per-domain rate limiter for polite crawling."""

from __future__ import annotations

import time
from collections import defaultdict
from urllib.parse import urlparse


class DomainRateLimiter:
    """Enforces a minimum delay between requests to the same domain.

    Domain delays can be provided at init time (typically from city config).
    """

    def __init__(
        self,
        default_delay: float = 2.0,
        domain_delays: dict[str, float] | None = None,
    ):
        self.default_delay = default_delay
        self.domain_delays = dict(domain_delays) if domain_delays else {}
        self._last_request: dict[str, float] = defaultdict(float)

    def delay_for(self, url: str) -> float:
        domain = urlparse(url).netloc
        return self.domain_delays.get(domain, self.default_delay)

    def wait(self, url: str) -> None:
        """Block until it's polite to request this URL."""
        domain = urlparse(url).netloc
        delay = self.delay_for(url)
        elapsed = time.monotonic() - self._last_request[domain]
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request[domain] = time.monotonic()
