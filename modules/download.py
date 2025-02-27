from typing import ClassVar, Optional
from os import path
from pathlib import Path
from modules.limit import ChronAmRateLimiter
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

class ChronAmDownloader:
    """Downloads and tracks progress for downloading XML, TXT, PDF, and JP2 files from [Chronicling America](https://chroniclingamerica.loc.gov/about/).
    
    Attributes:
        ids (dict[str, set[str]]): 
            a mapping of IDs to download progress; each key is an ID and each value is a set containing zero or more of `('xml', 'txt', 'pdf', 'jp2')` to indicate that the file with that extension has been downloaded.
        data_dir (str):
            a directory to write the downloaded files to.
        limiter (ChronAmRateLimiter):
            an object storing timestamps to prevent rate limiting from the API.
        executor (ThreadPoolExecutor | None): 
            an optional executor for executing downloads concurrently.
    """

    DATA_URL: ClassVar[str] = 'https://chroniclingamerica.loc.gov/lccn/'
    FILETYPES: ClassVar[list[str]] = ['xml', 'txt', 'pdf', 'jp2']

    def __init__(self, id_list: list[str], data_dir: str, limiter: ChronAmRateLimiter, executor: Optional[ThreadPoolExecutor]=None) -> None:
        """Initializes the object and validates attributes where applicable. Raises ValueError if attributes cannot be validated or fixed.
        
        Arguments:
            id_list  (list[str])                 : a list of IDs to be downloaded; IDs have the format `<lccn>/<YYYY>-<MM>-<DD>/ed-<edition_no>/seq-<page_no>/`
            data_dir (str)                       : a directory to write the downloaded files to
            limiter  (ChronAmRateLimiter)        : an object storing timestamps to prevent rate limiting from the API.
            executor (ThreadPoolExecutor | None) : an optional executor for executing downloads concurrently.

        """

        self.ids = {id: set[str]() for id in id_list}

        if not path.isdir(data_dir):
            raise FileNotFoundError(f'ERROR: provided data directory {data_dir} is not a directory.')
        self.data_dir = data_dir

        self.limiter = limiter
        self.executor = executor
    
    @staticmethod
    def from_file(filepath: str, data_dir: str, limiter: ChronAmRateLimiter, executor: Optional[ThreadPoolExecutor]=None, sep: str='\n') -> 'ChronAmDownloader':
        """Initializes a `ChronAmDownloader` object from a file of newline-separated IDs.
        
        Arguments:
            filepath (str)                       : the path to the file containing IDs.
            limiter  (ChronAmRateLimiter)        : an object storing timestamps to prevent rate limiting from the API.
            executor (ThreadPoolExecutor | None) : an optional executor for executing downloads concurrently.
            sep      (str)                       : the separator between IDs in the file, newline by default.

        Returns:
            _ (ChronAmDownloader) : a `ChronAmDownloader` initialized with the IDs in read from the file at `filepath`.
        
        """

        with open(filepath, 'r') as fp:
            id_list = fp.read().strip().split(sep)

        return ChronAmDownloader([id for id in id_list if id], data_dir, limiter, executor)

    @staticmethod
    def id_to_url(id: str, filetype: str) -> str:
        """Returns the `loc.gov` url associated with the provided ID and filetype."""
        base_url = 'https://chroniclingamerica.loc.gov/lccn/'
        if filetype in ('xml', 'txt'):
            return f'{base_url}{id}ocr.{filetype}'
        elif filetype in ('pdf', 'jp2'):
            return f'{base_url}{id[:-1]}.{filetype}'
        else:
            raise ValueError('ERROR: filetype must be one of ("xml", "txt", "pdf", "jp2")')
        
    def check_downloads(self, filetype: str) -> None:
        """Checks whether files of type `filetype` have been downloaded and updates `self.ids` accordingly."""

        exists = 0
        for id, types in self.ids.items():
            if path.exists(f'{path.join(self.data_dir, id[:-1])}.{filetype}'):
                types.add(filetype)
                exists += 1
        
        print(f'INFO: found {exists} files of type "{filetype}"; {len(self.ids) - exists} not found.')

    def download_file(self, id: str, filetype: str) -> str:
        """Downloads an XML, TXT, PDF, or JP2 file for the provided ID to `self.data_dir/<lccn>/<YYYY>-<MM>-<DD>/ed-<edition_no>/seq-<page_no>.<filetype>`.
        
        Arguments:
            id        (int)  : the ID of the file to be downloaded.
            filetype  (str)  : the type of file to be downloaded; must be one of `('xml', 'txt', 'pdf', 'jp2')`.

        Returns:
            _ (str) : the path to the downloaded file or `''` if the file is already present and `overwrite=False`.

        Raises:
            ValueError      : if the provided filetype is not one of `('xml', 'txt', 'pdf', 'jp2')`.
            ConnectionError : if no connection can be established to the host at loc.gov.

        """

        if filetype.lower() not in ChronAmDownloader.FILETYPES:
            raise ValueError('ERROR: filetype must be one of ("xml", "txt", "pdf", "jp2").')
        
        url = ChronAmDownloader.id_to_url(id, filetype)
        Path(path.join(self.data_dir, id.split('seq')[0])).mkdir(parents=True, exist_ok=True)
        filepath = f'{path.join(self.data_dir, id[:-1])}.{filetype}'
        try:
            with self.limiter.submit(requests.get, url) as response:
                response.raise_for_status()
                with open(filepath, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

        except requests.exceptions.HTTPError:
            raise requests.exceptions.HTTPError(f'ERROR: download for {url} failed with code {response.status_code}.')
        except ConnectionError:
            raise ConnectionError(f'ERROR: failed to establish connection with the host at loc.gov.')
        

        self.ids[id].add(filetype)
        return filepath

    def download_all(self, filetype: str, n_retries: int=3, overwrite: bool=False, allow_fail: bool=False) -> tuple[int, int, int]:
        """Downloads all the file of type `filetype` for each ID in `self.ids`.
        
        Arguments:
            filetype   (str)  : the type of file to be downloaded; must be one of `('xml', 'txt', 'pdf', 'jp2')`.
            n_retries  (int)  : the number of times per file to retry failed download and decoding.
            overwrite  (bool) : if True, overwrite files that have already been retrieved.
            allow_fail (bool) : if True, skip failed downloads and continue.

        Returns:
            _ (int, int int): A tuple with three `int` values: the number of files successfully downloaded, the number of files for which download failed (zero if `allow_errors=False`), and the number of files that were already present and skipped (zero if `overwrite=False`).
        """

        def download_file_with_retry(id: str, filetype: str) -> bool:
            """Downloads a file with `n_retries` attempts."""

            # this should never be raised; it is only declared to bind the variable
            exception = Exception()

            for i in range(n_retries):
                try:
                    self.download_file(id, filetype)
                    return True
                except Exception as e:
                    print(f'ERROR: download for id {id} failed; {n_retries - i - 1} attempts remaining.')
                    exception = e
            
            if allow_fail:
                return False
            else:
                raise exception
        
        self.check_downloads(filetype)

        downloaded, failed, skipped = 0, 0, 0
        if self.executor:
            workers = []

        for id, types in self.ids.items():

            if not overwrite and filetype in types:
                skipped += 1
                continue

            if self.executor:
                workers.append(self.executor.submit(download_file_with_retry, id, filetype))
            else:
                if download_file_with_retry(id, filetype):
                    downloaded += 1
                else:
                    failed += 1
        
        if self.executor:
            downloaded = sum(worker.result() for worker in as_completed(workers))
            failed = sum(not worker.result() for worker in as_completed(workers))

        return downloaded, failed, skipped


