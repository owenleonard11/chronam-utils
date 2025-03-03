from time import time, sleep
from threading import RLock
from typing import Callable, TypeVar

_R = TypeVar('_R')

class ChronAmRateLimiter:
    """This object keeps track of rate limits for the [loc.gov newspaper API](https://libraryofcongress.github.io/data-exploration/loc.gov%20JSON%20API/Chronicling_America/README.html#rate-limits).

    Attributes:
        burst_times (list[float])        : a list of request timestamps, oldest first; ensures compliance with burst limit.
        crawl_times (list[float])        : a list of request timestamps, oldest first; ensures compliance with crawl limit.
        lock        (RLock)              : provisions access to `burst_times` and `crawl_times`.
    """

    BURST_WINDOW, BURST_MAX = 60, 20
    CRAWL_WINDOW, CRAWL_MAX = 10, 20

    def __init__(self):
        self.burst_times: list[float] = []
        self.crawl_times: list[float] = []
        self.lock = RLock()
    
    def _clean_timestamps(self):
        """Removes timestamps from `self.burst_lock` and `self.crawl_lock` that are outside the respective windows."""
        now = time()
        self.burst_times = [t for t in self.burst_times if now - t < ChronAmRateLimiter.BURST_WINDOW]
        self.crawl_times = [t for t in self.crawl_times if now - t < ChronAmRateLimiter.CRAWL_WINDOW]

    def _record_request(self):
        """Records timestamp when request is made."""
        now = time()
        self.burst_times.append(now)
        self.crawl_times.append(now)

    def _check_wait(self) -> float:
        """Checks whether limits are exceeded and returns the time to wait."""
        self._clean_timestamps()
        burst_wait, crawl_wait = float(0), float(0)
 
        if len(self.burst_times) > ChronAmRateLimiter.BURST_MAX:
            burst_wait = max(0, self.burst_times[0] + ChronAmRateLimiter.BURST_WINDOW - time())

        if len(self.crawl_times) > ChronAmRateLimiter.CRAWL_MAX:
             crawl_wait = max(0, self.crawl_times[0] + ChronAmRateLimiter.CRAWL_WINDOW - time())
        
        return max(burst_wait, crawl_wait)

    def submit(self, f: Callable[..., _R], *args, **kwargs) -> _R:
        """Runs f(*args, **kwargs) as soon as possible without exceeding the rate limit."""

        wait = 0
        with self.lock:
            if not (wait := self._check_wait()):
                self._record_request()
        
        if wait:
            print(f'INFO: rate limit reached; waiting {wait:.2f} seconds.')
            sleep(wait)
            return self.submit(f, *args, **kwargs)
        else:
            return f(*args, **kwargs)
