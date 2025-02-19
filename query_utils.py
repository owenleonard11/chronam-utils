# requires python>=3.9

from urllib.parse import quote, quote_plus, unquote, unquote_plus
from dataclasses import dataclass, field
from datetime import date, datetime


SEARCH_URL_BASE = 'https://chroniclingamerica.loc.gov/search/pages/results/?'

STATES = ("", "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Piedmont","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virgin Islands","Virginia","Washington","West Virginia","Wisconsin","Wyoming")
LANGS  = ("", "ara","hrv","cze","dak","dan","eng","fin","fre","ger","ice","ita","lit","nob","pol","rum","slo","slv","spa","swe")
SORTS  = ('relevance', 'state', 'title', 'data')

@dataclass
class ChronAmQuery:
    """This dataclass stores parameters for a query to the [Chronicling America Search API](https://chroniclingamerica.loc.gov/about/api/#search). 

    Documentation for the API is limited. To best understand how searching Chronicling America works, I recommend playing around with the [advanced search function](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search) of the web interface.

    All attributes are optional. See `validate_query`, defined in this module, for more details on acceptable values.

    Attributes:
        ortext       (list[str]) : A list of single-word search terms; results containing any member of the list will be returned.
        andtext      (list[str]) : A list of single-word search terms; results containing all members of the list will be returned.
        phrasetext   (str)       : A multi-word search term; results containing exactly this term will be returned.
        proxtext      (list[str]) : A list of single-word search terms; results containing all members of the list within `proxdistance` words will be returned (see `proxdistance`).
        proxdistance (int)       : A nonnegative integer number of words representing the maximum distance allowed between members of `proxtext` (see `proxtext`).

        state          (str)  : A U.S. state or territory; see the `STATES` global defined in this module for a list of acceptable values.
        lccn           (str)  : A Library of Congress Control Number (LCCN) for a publication indexed by Chronicling America; these can be found from the [Title Search API](https://chroniclingamerica.loc.gov/about/api/#search).
        dateFilterType (str)  : `yearRange` for years only or `range` for full dates.
        date1          (date) : the start date for the search.
        date2          (date) : the end date for the search.
        sequence       (int)  : the page of the issues to search, starting with `1` for the frontpage.
        language       (str)  : one of a limited subset of ISO 639-2 (three-letter) language codes; see the `LANGS` global defined in this module for a list of acceptable values.
    
    """

    ortext       : list[str] = field(default_factory=lambda: [])
    andtext      : list[str] = field(default_factory=lambda: [])
    phrasetext   : str       = ''
    proxtext      : list[str] = field(default_factory=lambda: [])
    proxdistance : int       = 0

    state          : str  = ''
    lccn           : str  = ''
    dateFilterType : str  = 'yearRange'
    date1          : date = date(1756, 1, 1)
    date2          : date = date(1963, 12, 31)
    sequence       : int  = 0
    language       : str  = ''

def validate_and_clean_query(query: ChronAmQuery, verbose=False) -> bool:
    """Validate and clean (in place) a set of query parameters for use with the [Chronicling America advanced search API](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).
    
    Arguments:
        query   (ChronAmQuery) : a set of query parameters for the Chronicling America advanced search API.
        verbose (bool)         : if True, prints logging information to stdout.
    
    Returns:
        _ (bool) : `True` if validation is successful.
    
    Raises:
        ValueError: if any attributes of `query` have values incompatible with the API. 
        TypeError : if any attributes of `query` have an unexpected type.
        
    """

    def get_error_msg(attr: str, msg: str) -> str:
        return f'Query validation failed for attribute "{attr}": {msg}.'
    
    def log_cleaned(attr: str, before: str, fixed: str) -> None:
        if verbose: print(f'INFO: fixed attribute "{attr}": "{before}" -> "{fixed}"')
    
    def log_warning(msg: str) -> None:
        if verbose: print(f'WARNING: {msg}')

    for term_attr in ('ortext', 'andtext', 'proxtext'):
        if any(' ' in text for text in query.__getattribute__(term_attr)):
            raise ValueError(get_error_msg(term_attr, 'spaces not allowed in search term lists'))

    if query.proxdistance < 0:
        raise ValueError(get_error_msg('proxdistance', 'distance must be nonnegative'))
    
    if query.state and query.state not in STATES:
        state_fixed = query.state[0].upper() + query.state[1:].lower()
        if state_fixed not in STATES:
            raise ValueError(get_error_msg('state', f'{query.state} not recognized; see docs for list of acceptable values.'))
        log_cleaned('state', query.state, state_fixed) 
        query.state = state_fixed

    # this is not a full-fleshed lccn validator; it merely removes hyphens and checks for non-numeric characters
    lccn_fixed = query.lccn.replace('-', '').replace('sn', '')
    if not lccn_fixed and lccn_fixed.isnumeric():
        raise ValueError(get_error_msg('lccn', f'Library of Congress Control Numbers must contain only numeric characters'))
    if lccn_fixed != query.lccn:
        log_cleaned('lccn', query.lccn, lccn_fixed)
        query.lccn = lccn_fixed

    if query.dateFilterType not in ('range', 'yearRange'):
        raise ValueError(get_error_msg('dateFilterType', f'must be one of "range" or "yearRange"'))
    
    for date_attr in ('date1', 'date2'):
        if query.__getattribute__(date_attr) > date.today():
            log_warning(f'date provided for attribute "{date_attr}" is in the future')
    if query.date1 > query.date2:
        log_warning(f'start date is after end date; query will return 0 results')
    
    if query.sequence < 0:
        raise ValueError(get_error_msg('sequence', 'sequence must be nonnegative'))
    
    if query.language and query.language not in LANGS:
        language_fixed = query.language.lower()
        if language_fixed not in LANGS:
            raise ValueError(get_error_msg('language', f'{query.language} not recognized; see docs for list of acceptable values.'))
        
    return True

def url_to_query(url: str) -> ChronAmQuery:
    """Transform a URL obtained by using the [Chronicling America advanced search API](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search) into a ChronAmQuery object.

    This function does **not** store pagination details or sort criteria, even if they are present in the provided URL.
    
    Arguments:
        url (str)      : a URL corresponding to a page of search results from the Chronicling America advanced search API.
        verbose (bool) : if True, prints logging information to stdout.

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
        raise ValueError("failed to parse URL params")
    
    if query_dict.get('searchType', '') != 'advanced':
        raise ValueError("this function only handles URLs from the Chronicling America advanced search function at https://chroniclingamerica.loc.gov/#tab=tab_advanced_search")

    for key in ('sort', 'rows', 'page', 'format', 'searchType'):
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
        language       = query_dict.get('language', '')
    )

def query_to_params(query: ChronAmQuery) -> dict[str, str]:
    """Transform a ChronAmQuery object into a dictionary of formatted URL parameters for use with the [Chronicling America advanced search API](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).

    It is recommended to first validate and clean the query with `validate_and_clean_query`, defined in this module.
    
    Arguments:
        query (ChronAmQuery) : an object describing a set of query parameters for the Chronicling America advanced search API.

    Returns:
        _ (dict[str, str]) : a dictionary of formatted URL parameters for the Chronicling America advanced search API.

    """

    return {
        'ortext'       : '+'.join(quote(or_str, safe='') for or_str in query.ortext),
        'andtext'      : '+'.join(quote(and_str, safe='') for and_str in query.andtext),
        'phrasetext'   : quote_plus(query.phrasetext),
        'proxtext'     : '+'.join(quote(pro_str, safe='') for pro_str in query.proxtext),
        'proxdistance' : str(query.proxdistance or ''),

        'state'          : quote_plus(query.state),
        'lccn'           : query.lccn,
        'dateFilterType' : query.dateFilterType,
        'date1'          : quote(query.date1.strftime('%Y') if query.dateFilterType == 'yearRange' else query.date1.strftime('%m/%d/%Y'), safe=''),
        'date2'          : quote(query.date2.strftime('%Y') if query.dateFilterType == 'yearRange' else query.date2.strftime('%m/%d/%Y'), safe=''),
        'sequence'       : str(query.sequence or ''),
        'language'       : query.language,
    }

def query_to_url(query: ChronAmQuery, sort: str = 'relevance', rows: int = 50, verbose=False) -> str:
    """Transform a ChronAmQuery object into a URL for retrieving JSON-formatted results from the [Chronicling America advanced search API](https://chroniclingamerica.loc.gov/#tab=tab_advanced_search).

    It is recommended to first validate and clean the query with `validate_and_clean_query`, defined in this module.
    
    Arguments:
        query   (ChronAmQuery) : an object describing a set of query parameters for the Chronicling America advanced search API.
        sort    (str)          : determines ordering of search results; one of 'relevance', 'state', 'title', 'date'.
        rows    (int)          : the number of items to retrieve per page; must be a positive integer.
        verbose (bool)         : if True, prints logging information to stdout.

    Returns:
        _ (str) : a parameterized URL for accessing results from the query as JSON.

    """

    if sort not in SORTS:
        raise ValueError(f'Parameter "sort" must be one of "relevance", "state", "title", "data')
    
    if rows < 1:
        raise ValueError(f'Parameter "rows" must be a positive integer')
    
    if rows > 100 and verbose:
        print('WARNING: excessively large values for parameter "rows" may cause timeouts or long wait times')

    query_params = query_to_params(query)
    url_params = {
        'sort'       : sort,
        'rows'       : str(rows),
        'format'     : 'json',
        'searchType' : 'advanced'
    }
    return SEARCH_URL_BASE + '&'.join(f'{key}={value}' for key, value in (query_params | url_params).items())
