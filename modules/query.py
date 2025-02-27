from modules.limit import ChronAmRateLimiter
from typing import Any, Optional, ClassVar
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from urllib.parse import quote, quote_plus, unquote, unquote_plus
from json import dump
import requests
from threading import Lock

SEARCH_URL_BASE : str = 'https://chroniclingamerica.loc.gov/search/pages/results/?'

class ChronAmQuery:
    """Stores parameters for a query to [Chronicling America Advanced Search](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search);
    attributes correspond to url parameters. 

    Documentation for the API is limited. To best understand how searching Chronicling America works, I recommend playing around with the [advanced search function](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search) of the web interface.
    You can also take advantage of the `from_url` utility, which will automatically initialize a `ChronAmQuery` from a search result URL.

    All attributes are optional. See `validate_query`, defined in this module, for more details on acceptable values.

    Attributes:
        ortext (list[str]): 
            a list of single-word search terms; results containing any member of the list will be returned.
        andtext (list[str]):
            a list of single-word search terms; results containing all members of the list will be returned.
        phrasetext   (str): 
            a multi-word search term; results containing exactly this term will be returned.
        proxtext     (list[str]): 
            a list of single-word search terms; results containing all members of the list within `proxdistance` words will be returned (see `proxdistance`).
        proxdistance (int): 
            a nonnegative integer number of words representing the maximum distance allowed between members of `proxtext` (see `proxtext`).
        state (str): 
            a U.S. state or territory; see the `STATES` class variable for a list of acceptable values.
        lccn (str): 
            a Library of Congress Control Number (LCCN) for a publication indexed by Chronicling America; these can be found from the [Title Search API](https://chroniclingamerica.loc.gov/about/api/#search).
        dateFilterType (str): 
            `yearRange` for years only or `range` for full dates.
        date1 (date): 
            the start date for the search.
        date2 (date): 
            the end date for the search.
        sequence (int): 
            the page of the issues to search, starting with `1` for the frontpage.
        language (str): 
            one of a limited subset of ISO 639-2 (three-letter) language codes; see the `LANGS` class variable for a list of acceptable values.
        sort (str):
            determines ordering of search results; one of 'relevance', 'state', 'title', 'date'.
        
        max_results (int):
            the maximum number of IDs to retrieve; `0` to retrieve all.
        n_results (int):
            the total number of results retrieved from the query; -1 indicates that the value is unknown.
        results (dict[int, str]): 
            the results of the query, formatted as a mapping of indices (starting with 1) to newspaper page IDs.
        _results_lock (Lock):
            provisions access to `results`.

        desc (str):
            a text description of the query

    """

    STATES : ClassVar[list[str]] = ["", "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Piedmont","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virgin Islands","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]
    LANGS  : ClassVar[list[str]] = ["", "ara","hrv","cze","dak","dan","eng","fin","fre","ger","ice","ita","lit","nob","pol","rum","slo","slv","spa","swe"]
    SORTS  : ClassVar[list[str]] = ['relevance', 'state', 'title', 'data']
    PARAMS : ClassVar[list[str]] = ['ortext', 'andtext', 'phrasetext', 'proxtext', 'proxdistance', 'state', 'lccn', 'dateFilterType', 'date1', 'date2', 'sequence', 'language', 'sort']

    def __init__(  
        self,
        ortext: Optional[list[str]] = None,
        andtext: Optional[list[str]] = None,
        phrasetext: str = '',
        proxtext: Optional[list[str]] = None,
        proxdistance: int       = 0,
        state: str  = '',
        lccn: str  = '',
        dateFilterType: str  = 'yearRange',
        date1: date = date(1756, 1, 1),
        date2: date = date.today(),
        sequence: int  = 0,
        language: str  = '',
        sort: str = 'relevance',
        max_results: int = 50,
        desc: str = '' 
    ) -> None:
        """Initializes the object and validates attributes where applicable. Raises ValueError if attributes cannot be validated or fixed."""
        
        def get_error_msg(attr: str, msg: str) -> str:
            return f'Query validation failed for attribute "{attr}": {msg}.'

        self.ortext, self.andtext, self.proxtext = ortext or [], andtext or [], proxtext or []
        for term_attr in ('ortext', 'andtext', 'proxtext'):
            if any(' ' in text for text in self.__getattribute__(term_attr)):
                raise ValueError(get_error_msg(term_attr, 'spaces not allowed in search term lists'))
        
        self.phrasetext = phrasetext

        self.proxdistance = proxdistance
        if self.proxdistance < 0:
            raise ValueError(get_error_msg('proxdistance', 'distance must be nonnegative'))
        
        self.state = state
        if self.state and self.state not in ChronAmQuery.STATES:
            state_fixed = self.state[0].upper() + self.state[1:].lower()
            if state_fixed not in ChronAmQuery.STATES:
                raise ValueError(get_error_msg('state', f'{self.state} not recognized; see docs for list of acceptable values.'))
            print(f'INFO: fixed attribute "state"; {self.state} -> {state_fixed}') 
            self.state = state_fixed

        # this is not a full-fleshed lccn validator; it merely removes hyphens and checks for non-numeric characters
        self.lccn = lccn
        lccn_fixed = self.lccn.replace('-', '').replace('sn', '')
        if not lccn_fixed and lccn_fixed.isnumeric():
            raise ValueError(get_error_msg('lccn', f'Library of Congress Control Numbers must contain only numeric characters'))
        if lccn_fixed != self.lccn:
            self.lccn = lccn_fixed

        self.dateFilterType = dateFilterType
        if self.dateFilterType not in ('range', 'yearRange'):
            raise ValueError(get_error_msg('dateFilterType', f'must be one of "range" or "yearRange"'))
        
        self.date1, self.date2 = date1, date2
        for date_attr in ('date1', 'date2'):
            if self.__getattribute__(date_attr) > date.today():
                print(f'WARNING: date provided for attribute "{date_attr}" is in the future')
        if self.date1 > self.date2:
            print(f'WARNING: start date is after end date; query will return 0 results')
        
        self.sequence = sequence
        if self.sequence < 0:
            raise ValueError(get_error_msg('sequence', 'sequence must be nonnegative'))
        
        self.language = language
        if self.language and self.language not in ChronAmQuery.LANGS:
            language_fixed = self.language.lower()
            if language_fixed not in ChronAmQuery.LANGS:
                raise ValueError(get_error_msg('language', f'{self.language} not recognized; see docs for list of acceptable values.'))
        
        self.sort = sort
        if self.sort not in ChronAmQuery.SORTS:
            raise ValueError(f'Parameter "sort" must be one of "relevance", "state", "title", "data')
        
        self.max_results = max_results
        self.n_results = -1
        self.results: dict[int, str] = {}
        self._results_lock: Lock = Lock()
        
        self.desc = desc or str(id(self))

    def __setattr__(self, name: str, value: Any) -> None:
        """Resets results if query parameters are changed."""
        if getattr(self, 'n_results', -1) != -1 and name in ChronAmQuery.PARAMS:
            self.results.clear()
            super().__setattr__('n_results', -1)
            print(f'INFO: query parameter {name} changed, stored results cleared.')

        super().__setattr__(name, value)
    
    def __str__(self) -> str:
        """Returns a string representation of the query."""
        param_string = '\n'.join(f'\t{param}: {self.__getattribute__(param)}' for param in type(self).PARAMS)
        return f'{type(self).__name__}(\n\tdescription: "{self.desc}"\n{param_string}\n)'
    
    def __repr__(self) -> str:
        """See `__str__`."""
        return str(self)
    
    @staticmethod
    def _parse_date(date: str, date_type: str, dateFilterType: str) -> date:
            """Parses a string date into a `date` object depending on the `dateFilterType` and whether `date_type` is `'start'` or `'end'`."""
            if dateFilterType == 'yearRange':
                if date_type == 'start':
                    return datetime.strptime(unquote(date), '%Y').date()
                elif date_type == 'end':
                    return datetime.strptime(str(int(unquote(date)) + 1), '%Y').date() - timedelta(1)
                else:
                    raise ValueError(f'ERROR: date_type must be one of "start" or "end".')
            elif dateFilterType == 'range':
                return datetime.strptime(unquote(date), '%m/%d/%Y').date()
            else:
                raise ValueError(f'ERROR: Failed to parse date {date} with dateFilterType {dateFilterType}.')
    
    @classmethod
    def from_url(cls, url: str) -> 'ChronAmQuery':
        """Initializes a ChronAmQuery object from a URL obtained by using the [Chronicling America advanced search API](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).

        This function does **not** store pagination details, even if they are present in the provided URL.
        
        Arguments:
            url (str)      : a URL corresponding to a page of search results from the Chronicling America advanced search API.

        Returns:
            _ (ChronAmQuery): an object describing a set of query parameters for the Chronicling America advanced search API.

        Raises:
            ValueError: if the URL is malformed or its parameters are badly formatted.

        """

        url = url.replace('https://chroniclingamerica.loc.gov/search/pages/results/?', '')
        url = url.replace('#tab=tab_advanced_search', '')

        try:
            query_dict: dict[str, str] = {key: value for key, value in (param.split('=') for param in url.split('&'))}
        except:
            raise ValueError("Failed to parse URL params.")
        
        if query_dict.get('searchType', '') != 'advanced':
            raise ValueError("This function only handles URLs from the Chronicling America advanced search at https://chroniclingamerica.loc.gov/#tab=tab_advanced_search. Use ChronAmBasicQuery for URLs from the basic search interface.")

        for key in ('rows', 'page', 'format', 'searchType'):
            query_dict.pop(key, None)

        return ChronAmQuery(
            ortext       = [unquote(or_str) for or_str in query_dict.get('ortext', '').split('+')],
            andtext      = [unquote(or_str) for or_str in query_dict.get('andtext', '').split('+')],
            phrasetext   = unquote_plus(query_dict['phrasetext']),
            proxtext     = [unquote(pro_str) for pro_str in query_dict.get('proxtext', '').split('+')],
            proxdistance = int(query_dict.get('proxdistance', '0')),

            state          = unquote_plus(query_dict.get('state', '')),
            lccn           = query_dict.get('lccn', ''),
            dateFilterType = query_dict['dateFilterType'],
            date1          = cls._parse_date(query_dict['date1'], 'start', query_dict['dateFilterType']), 
            date2          = cls._parse_date(query_dict['date2'], 'end', query_dict['dateFilterType']), 
            sequence       = int(query_dict.get('sequence', '0')),
            language       = query_dict.get('language', ''),

            sort = query_dict.get('sort', 'relevance')
        )
    
    @property
    def url(self) -> str:
        """Returns a URL for retrieving JSON-formatted query results."""

        url_params = {
            'ortext': '+'.join(quote(or_str, safe='') for or_str in self.ortext),
            'andtext': '+'.join(quote(and_str, safe='') for and_str in self.andtext),
            'phrasetext': quote_plus(self.phrasetext),
            'proxtext': '+'.join(quote(pro_str, safe='') for pro_str in self.proxtext),
            'proxdistance': str(self.proxdistance or ''),

            'state': quote_plus(self.state),
            'lccn': self.lccn,
            'dateFilterType': self.dateFilterType,
            'date1': quote(self.date1.strftime('%Y') if self.dateFilterType == 'yearRange' else self.date1.strftime('%m/%d/%Y'), safe=''),
            'date2': quote(self.date2.strftime('%Y') if self.dateFilterType == 'yearRange' else self.date2.strftime('%m/%d/%Y'), safe=''),
            'sequence': str(self.sequence or ''),
            'language': self.language,
            'sort': self.sort,

            'format': 'json',
            'searchType': 'advanced'
        }

        return SEARCH_URL_BASE + '&'.join(f'{key}={value}' for key, value in (url_params).items())
    
    @property
    def ids(self) -> list[str]:
        """Returns a list of ids corresponding to a digitized newspaper page in the Chronicling America database."""
        return list(self.results.values())
    
    def retrieve_page(self, page: int, page_size: int, limiter: ChronAmRateLimiter) -> int:
        """Downloads, decodes and records a single page of query results.
        
        Arguments:
            page      (int)                : the page number (indexed from 1) to retrieve.
            page_size (int)                : the number of results per page.
            limiter   (ChronAmRateLimiter) : an object storing timestamps to prevent rate limiting from the API.
        
        Returns:
            _ (int) : the total number items written to `result`.
        
        Raises:
            ValueError      : if downloading or JSON decoding fails.
            ConnectionError : if no connection can be established to the host at loc.gov.
        
        """

        page_url = f'{self.url}&page={page}&rows={page_size}'
        
        try:
            response = limiter.submit(requests.get, page_url)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise ValueError(f'ERROR: download for {page_url} failed with code {response.status_code}')
        except ConnectionError:
            raise ConnectionError(f'ERROR: failed to establish connection with the host at loc.gov.')
        
        try:
            response_json = response.json()
        except ValueError:
            raise ValueError(f'ERROR: failed to parse JSON for {page_url}.')
        
        for exp_prop, exp_type in [('totalItems', int), ('endIndex', int), ('startIndex', int), ('items', list)]:
            if exp_prop not in response_json or type(response_json[exp_prop]) is not exp_type:
                raise ValueError(f'ERROR: unrecognized JSON response format for {page_url}.')

        if self.n_results == -1:
            print(f'INFO: found {response_json["totalItems"]} results for query {self.desc}')
            if self.max_results == 0:
                self.n_results = response_json['totalItems']
            else:
                self.n_results = min(self.max_results, response_json['totalItems'])

        written = 0
        with self._results_lock:
            for i, item in enumerate(response_json['items']):
                index = i + response_json['startIndex']
                if not self.max_results or index <= self.max_results:
                    self.results[index] = item['id'].replace('/lccn/', '') 
                    written += 1

        print(f'INFO: updated query "{self.desc}" with {written} items.')
            
        return written
    
    def retrieve_all(self, page_size: int, limiter: ChronAmRateLimiter, executor: Optional[ThreadPoolExecutor]=None, n_retries: int = 3, overwrite: bool = False):
        """Populates `result` with newspaper page IDs by running a [Chronicling America advanced search](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).

        Arguments:
            limiter      (ChronAmRateLimiter)        : an object storing timestamps to prevent rate limiting from the API.
            page_size    (int)                       : the number of results to be retrieved per page.
            executor     (ThreadPoolExecutor | None) : an optional executor for executing page retrievals concurrently.
            n_retries    (int)                       : the number of times per page to retry failed download and decoding.
            overwrite    (bool)                      : if True, overwrite pages that have already been retrieved.

        Returns:
            _ (int) : the total number of items written to `result`.

        Raises:
            ValueError : if page download and decoding fails after `n_retries` attempts.

        """

        def retrieve_page_with_retry(page: int) -> int:
            """Retrieves page `page` with `n_retries` attempts."""
            for i in range(n_retries):
                try:
                    return self.retrieve_page(page, page_size, limiter)
                except ValueError as e:
                    print(f'ERROR: download for query "{self.desc}" page {page} failed; {n_retries - i - 1} attempts remaining.')
                    msg = str(e)
            
            raise ValueError(msg)
        
        page, written, workers = 1, 0, []

        ## ensure that n_results is known before spawning threads
        if self.n_results == -1:
            written = retrieve_page_with_retry(1)
            page = 2
            remaining = self.n_results - page_size
        else:
            remaining = self.n_results
        
        while remaining is None or remaining > 0:
            indices = range(page_size * (page - 1) + 1, min(self.max_results, page_size * page  + 1))
            is_full = all(self.results.get(index, '') for index in indices if index <= self.n_results)

            if is_full and not overwrite:
                print(f'INFO: page {page} already present for query "{self.desc}."')
            else:
                if executor:
                    workers.append(executor.submit(retrieve_page_with_retry, page))
                else:
                    written += retrieve_page_with_retry(page)
            
            page += 1
            remaining -= page_size
        
        if executor:
            return sum(worker.result() for worker in as_completed(workers))
        else:
            return written

    def dump_json(self, filepath: str) -> None:
        """Writes the query results to `filepath` as JSON."""
        with open(filepath, 'w') as fp:
            dump(self.results, fp, indent=4)

    def dump_txt(self, filepath: str) -> int:
        """Writes the query results to a text file as a newline-separated list of IDs. Returns the number of IDs written."""
        with open(filepath, 'w') as fp:
            return sum(bool(fp.write(f'{id}\n')) for id in self.results.values())

class ChronAmBasicQuery(ChronAmQuery):
    """A subclass of `ChronAmQuery` that instead uses the more limited [Chronicling America Basic Search](https://chroniclingamerica.loc.gov/#tab=tab_search)."""
    
    PARAMS : ClassVar[list[str]] = ['state', 'date1', 'date2', 'proxtext']

    def __init__(
            self, 
            state: str = '', 
            date1: str = '1756',
            date2: str = date.today().strftime('%Y'),
            proxtext: list[str] = [],
            sort: str = 'relevance',
            max_results: int = 0,
            desc: str =''
        ) -> None:
        """Initializes the object using the `ChronAmQuery` initializer with only basic parameters specified.
        
        Arguments:
            state    (str)       : a U.S. state or territory; see the `STATES` class variable for a list of acceptable values.
            date1    (str)       : a four-character string representing the year to start the search, e.g. '1903.'
            date2    (str)       : a four-character string representing the year to end the search, e.g. '1917.'
            proxtext (list[str]) : a list of single-word search terms.
            sort     (str)       : determines ordering of search results; one of 'relevance', 'state', 'title', 'date'.
            max_results (int)    : the maximum number of IDs to retrieve; `0` to retrieve all.
            desc (str)           : a text description of the query

        """
        try:
            parsed_date1 = super(type(self), type(self))._parse_date(date1, 'start', 'yearRange')
            parsed_date2 = super(type(self), type(self))._parse_date(date2, 'end', 'yearRange')
        except ValueError:
            raise ValueError(f'ERROR: Failed to parse dates ({date1}, {date2}).')

        super().__init__(state=state, proxtext=proxtext, date1=parsed_date1, date2=parsed_date2, sort=sort, max_results=max_results, desc=desc)
    
    @classmethod
    def from_url(cls, url: str) -> 'ChronAmBasicQuery':
        """Initializes a ChronAmBasicQuery object from a URL obtained by using [Chronicling America basic search](https://chroniclingamerica.loc.gov/#tab=tab_search).

        This function does **not** store pagination details, even if they are present in the provided URL.
        
        Arguments:
            url (str)      : a URL corresponding to a page of search results from the Chronicling America basic search API.

        Returns:
            _ (ChronAmQuery): an object describing a set of query parameters for the Chronicling America basic search API.

        Raises:
            ValueError: if the URL is malformed or its parameters are badly formatted.

        """

        url = url.replace('https://chroniclingamerica.loc.gov/search/pages/results/?', '')
        url = url.replace('#tab=tab_search', '')

        try:
            query_dict: dict[str, str] = {key: value for key, value in (param.split('=') for param in url.split('&'))}
        except:
            raise ValueError("Failed to parse URL params.")
        
        if query_dict.get('searchType', '') != 'basic':
            raise ValueError("This function only handles URLs from the Chronicling America basic search at https://chroniclingamerica.loc.gov/#tab=tab_search. Use ChronAmQuery for URLs from the advanced search interface.")

        for key in ('rows', 'page', 'format', 'searchType'):
            query_dict.pop(key, None)

        return ChronAmBasicQuery(
            proxtext = [unquote(pro_str) for pro_str in query_dict.get('proxtext', '').split('+')],
            state    = unquote_plus(query_dict.get('state', '')),
            date1    = query_dict['date1'],
            date2    = query_dict['date2'],
            sort = query_dict.get('sort', 'relevance')
        )

    @property
    def url(self) -> str:
        """Returns a URL for retrieving JSON-formatted query results."""

        url_params = {
            'proxtext': '+'.join(quote(pro_str, safe='') for pro_str in self.proxtext),
            'state': quote_plus(self.state),
            'dateFilterType': self.dateFilterType,
            'date1': quote(self.date1.strftime('%m/%d/%Y'), safe=''),
            'date2': quote(self.date2.strftime('%m/%d/%Y'), safe=''),

            'format': 'json',
            'searchType': 'advanced'
        }

        return SEARCH_URL_BASE + '&'.join(f'{key}={value}' for key, value in (url_params).items())

class ChronAmMultiQuery:
    """Handles rate limiting and concurrency for multiple ChronAmQuery instances.

    Attributes:
        queries  (list[ChronAmQuery])        : a list of ChronAmQuery objects for defining query paremeters and storing results.
        limiter  (ChronAmRateLimiter)        : an object storing timestamps to prevent rate limiting from the API.
        executor (ThreadPoolExecutor | None) : an optional executor for executing page retrievals concurrently.
    
    """

    def __init__(self, queries: list[ChronAmQuery], limiter: ChronAmRateLimiter, executor: Optional[ThreadPoolExecutor] = None):
        self.queries  = queries
        self.limiter  = limiter
        self.executor = executor
    
    def __getitem__(self, index) -> ChronAmQuery:
        """Allows indexing for retrieval of individual queries."""
        return self.queries[index]
    
    def retrieve_all(self, page_size: int, n_retries: int=3, overwrite: bool=False) -> int:
        """Retrieves and stores results for all queries, returns the total number of items written."""
        return sum(query.retrieve_all(page_size, self.limiter, self.executor, n_retries, overwrite) for query in self.queries)

    def dump_json(self, filepath: str) -> None:
        """Writes the query results to `filepath` as JSON."""
        with open(filepath, 'w') as fp:
            dump({ query.desc: query.results for query in self.queries }, fp, indent=4)

    def dump_txt(self, filepath: str, allow_duplicates: bool=True) -> int:
        """Writes the query results to a text file as a newline-separated list of IDs, omitting duplicates if `allow_duplicates=True`. Returns the number of IDs written."""
        written = set()
        with open(filepath, 'w') as fp:
            for query in self.queries:
                for id in query.results.values():
                    if not allow_duplicates and query in set():
                        continue
                    else:
                        fp.write(f'{id}\n')
                        written.add(id)
            
            return len(written)

