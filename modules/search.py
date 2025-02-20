from modules.limit import ChronAmRateLimiter
from typing import Optional, ClassVar
from dataclasses import dataclass, field
from datetime import date, datetime
from urllib.parse import quote, quote_plus, unquote, unquote_plus
from time import sleep
import requests

SEARCH_URL_BASE : str = 'https://chroniclingamerica.loc.gov/search/pages/results/?'

class ChronAmQuery:
    """Stores parameters for a query to the [Chronicling America Search API](https://chroniclingamerica.loc.gov/about/api/#search). 

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
            a U.S. state or territory; see the `STATES` global defined in this module for a list of acceptable values.
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
            one of a limited subset of ISO 639-2 (three-letter) language codes; see the `LANGS` global defined in this module for a list of acceptable values.
        sort (str):
            determines ordering of search results; one of 'relevance', 'state', 'title', 'date'.
        
        max_results (int):
            the maximum number of IDs to retrieve; `0` to retrieve all.
        n_results (int):
            the total number of results retrieved from the query; -1 indicates that the value is unknown.
        results (dict[int, str]): 
            the results of the query, formatted as a mapping of indices (starting with 1) to newspaper page IDs.

        desc (str):
            a text description of the query

    """

    STATES : ClassVar[list[str]] = ["", "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Piedmont","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virgin Islands","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]
    LANGS  : ClassVar[list[str]] = ["", "ara","hrv","cze","dak","dan","eng","fin","fre","ger","ice","ita","lit","nob","pol","rum","slo","slv","spa","swe"]
    SORTS  : ClassVar[list[str]] = ['relevance', 'state', 'title', 'data']

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
        date2: date = date(1963, 12, 31),
        sequence: int  = 0,
        language: str  = '',
        sort: str = 'relevance',
        max_results: int = 0,
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
        
        self.desc = desc or str(id(self))
    
    @staticmethod
    def from_url(url: str) -> 'ChronAmQuery':
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
            raise ValueError("This function only handles URLs from the Chronicling America advanced search function at https://chroniclingamerica.loc.gov/#tab=tab_advanced_search.")

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
            date1          = datetime.strptime(unquote(query_dict['date1']), '%m/%d/%Y').date(),
            date2          = datetime.strptime(unquote(query_dict['date2']), '%m/%d/%Y').date(),
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
    
    def retrieve_page(self, page: int, limiter: ChronAmRateLimiter, page_size: int) -> int:
        """Downloads, decodes and records a single page of query results.
        
        Arguments:
            page      (int) : the page number (indexed from 1) to retrieve.
            page_size (int) : the number of results per page.
        
        Returns:
            _ (int) : the total number items written to `result`.
        
        Raises:
            ValueError : if downloading or JSON decoding fails.
        
        """

        page_url = f'{self.url}&page={page}&rows={page_size}'

        if (wait := limiter.check()):
            print(f'INFO: rate limit reached; waiting {wait} seconds.')
            sleep(wait + 0.1)
        
        try:
            limiter.record()
            response = requests.get(page_url)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise ValueError(f'ERROR: download for {page_url} failed with code {response.status_code}')
        
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
        for i, item in enumerate(response_json['items']):
            index = i + response_json['startIndex']
            if not self.max_results or index <= self.max_results:
                self.results[index] = item['id'].replace('/lccn/', '') 
                written += 1
            
        print(f'INFO: updated query "{self.desc}" with {written} items.')
        return written
    
    def retrieve_all(self, limiter: ChronAmRateLimiter, page_size: int, n_retries: int = 3, max_workers: int = 4, allow_errors: bool = False, overwrite: bool = False):
        """Populates `result` with newspaper page IDs by running a [Chronicling America advanced search](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).

        Arguments:
            limiter      (ChronAmRateLimiter) : an object storing timestamps to prevent rate limiting from the API.
            page_size    (int)                : the number of results to be retrieved per page.
            n_retries    (int)                : the number of times per page to retry failed download and decoding.
            max_workers  (int)                : the maximum number of threads to use for concurrent retrieval of IDs.
            allow_errors (bool)               : if True, continue even if page download and decoding fail `n_retry` times.
            overwrite    (bool)               : if True, overwrite pages that have already been retrieved.

        Returns:
            _ (int) : the total number items written to `result`.

        Raises:
            ValueError : if `allow_errors=False` and page download and decoding fails after `n_retry` attempts.

        """

        def retrieve_page_with_retry(page) -> int:
            for _ in range(n_retries):
                try:
                    return self.retrieve_page(page, limiter, page_size)
                except ValueError as e:
                    error_msg = str(e)
                
            if allow_errors:
                print(f'ERROR: download for query "{self.desc}" page {page} failed {n_retries} times, skipping page and proceeding.')
                return 0
    
            raise ValueError(error_msg)
        
        written = 0
        written += retrieve_page_with_retry(1)
        if self.n_results <= page_size:
            return self.n_results

        for page in range(1, self.n_results // page_size + 2):
            written += retrieve_page_with_retry(page)
        
        return written
