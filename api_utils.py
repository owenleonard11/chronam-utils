import requests
from concurrent import futures
from threading import Lock
import time

from query_utils import ChronAmQuery, query_to_url

class RateLimitExecutor:
    """This object keeps track of rate limits for the [loc.gov newspaper API](https://libraryofcongress.github.io/data-exploration/loc.gov%20JSON%20API/Chronicling_America/README.html#rate-limits).

    Attributes:
        burst_times (list[float])        : a list of request timestamps, oldest first; ensures compliance with burst limit.
        crawl_times (list[float])        : a list of request timestamps, oldest first; ensures compliance with crawl limit.
        burst_lock  (Lock)               : provisions access to `burst_times`.
        crawl_lock  (Lock)               : provisions access to `crawl_times`.
    """

    BURST_WINDOW, BURST_MAX = 60, 20
    CRAWL_WINDOW, CRAWL_MAX = 10, 20

    def record(self):
        """Record timestamp when request is made."""
        with self.burst_lock: self.burst_times.append(time.time())
        with self.crawl_lock: self.crawl_times.append(time.time())

    def clean(self):
        """Remove stale timestamps."""
        now = time.time()
        with self.burst_lock: self.burst_times = [t for t in self.burst_times if now - t < RateLimitExecutor.BURST_WINDOW]
        with self.crawl_lock: self.crawl_times = [t for t in self.crawl_times if now - t < RateLimitExecutor.CRAWL_WINDOW]

    def check(self) -> float:
        """Check whether limits are exceeded, return the time to wait."""
        burst_wait, crawl_wait = float(0), float(0)
        with self.burst_lock:
            if len(self.burst_times) > RateLimitExecutor.BURST_MAX:
                burst_wait = max(0, self.burst_times[0] + RateLimitExecutor.BURST_WINDOW - time.time())
        with self.crawl_lock:
            if len(self.crawl_times) > RateLimitExecutor.CRAWL_MAX:
                crawl_wait = max(0, self.crawl_times[0] + RateLimitExecutor.CRAWL_WINDOW - time.time())

        return max(burst_wait, crawl_wait)

    def __init__(self):
        self.burst_times: list[float] = []
        self.crawl_times: list[float] = []
        self.burst_lock = Lock()
        self.crawl_lock = Lock()

def query_to_ids(query: ChronAmQuery, limiter: RateLimitExecutor = RateLimitExecutor(), sort: str = 'relevance', batch_size:int = 50, max_batches:int = 0, allow_errors=False, n_retries: int = 3, max_workers: int = 4, verbose = False) -> list[str]:
    """Retrieve a list of IDs corresponding to the results of a [Chronicling America advanced search](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search) described by a ChronAmQuery object.

    It is recommended to first validate and clean the query with `validate_and_clean_query`, defined in this module.

    Arguments:
        query        (ChronAmQuery)     : an object describing a set of query parameters for the Chronicling America advanced search API.
        limiter      (RateLimitManager) : limits requests to the Chronicling America API; leave blank for a fresh instance.
        sort         (str)              : determines ordering of search results; one of 'relevance', 'state', 'title', 'date'.
        batch_size   (int)              : the number of results to be retrieved per page.
        max_batches  (int)              : the number of pages to retrieve; if 0 all pages are retrieved.
        n_retries    (int)              : the number of times per page to retry failed download and decoding.
        allow_errors (bool)             : if True, continue even if page download and decoding fail n_retry times.
        max_workers  (int)              : the maximum number of threads to use for concurrent retrieval of IDs.
        verbose      (bool)             : if True, prints logging information to stdout.

    Returns:
        _ (list[str]) : a list containing retrieved IDs of pages in the Chronicling America databse, with the format `<lccn>/<YYYY>-<MM>-<DD>/ed-<edition>/seq-<page_number>`

    Raises:
        ValueError : if `allow_errors=False` and page download and decoding fails after n_retry attempts.

    """

    total_items = -1
    query_url   = query_to_url(query, sort=sort, rows=batch_size, verbose=False)

    def attempt_download_and_decode_page(n: int) -> list[str]:
        """Attempt to download and decode a single page of results, raising `ValueError` if either step fails."""
        nonlocal total_items
        page_url = f'{query_url}&page={n}'

        try:
            if (wait := limiter.check()):
                if verbose:
                    print(f'INFO: rate limit reached; waiting {wait} seconds.')
                time.sleep(wait + 0.1)
            limiter.record()

            response = requests.get(page_url)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise ValueError(f'ERROR: download for {page_url} failed with code {response.status_code}')
        
        try:
            response_json = response.json()
        except ValueError:
            raise ValueError(f'ERROR: failed to parse JSON for {page_url}.')
        
        if 'items' not in response_json or 'totalItems' not in response_json or type(response_json['items']) is not list:
            raise ValueError(f'ERROR: unexpected JSON response for {page_url}.')
        
        if verbose and total_items == -1:
            print(f'INFO: found {response_json["totalItems"]} total items.')
        total_items = response_json['totalItems']
        
        if verbose:
            print(f'INFO: retrieved {len(response_json["items"])} ids for page {n}.')
        return [item['id'].replace('/lccn/', '') for item in response_json['items']]
        
    def download_and_decode_page(n: int) -> list[str]:
        """Attempt to download and decode a single page of results with `n_retry` retries, raising `ValueError` on failure if `allow_errors=False`; if `allow_errors=True`, an empty list will instead by returned on failure."""
        for _ in range(n_retries):
            try:
                return attempt_download_and_decode_page(n)
            except Exception as e:
                error_msg = str(e)
        
        if allow_errors:
            if verbose:
                print(f'ERROR: download for page {n} failed {n_retries} times, omitting page and continuing.')
            return []
        
        raise ValueError(error_msg)
    
    # we need to get the first page first, so we can find the total number of items retrieved
    page_one = download_and_decode_page(1)
    if total_items < batch_size or not page_one:
        return page_one
    
    available_batches = total_items // batch_size + 1
    n_batches = min(available_batches, max_batches) if max_batches else available_batches

    id_dict = { 1: page_one }
    with futures.ThreadPoolExecutor(max_workers) as executor:
        batches = {n: executor.submit(download_and_decode_page, n) for n in range(2, n_batches + 1)}

    for n, id_future in batches.items():
        id_dict[n] = id_future.result()
    
    id_lists = [id_dict[n] for n in range(1, n_batches + 1)]
    if verbose:
        print(f'INFO: {id_lists.count([])} of {n_batches} failed.')

    return [id for id_list in id_lists for id in id_list]
