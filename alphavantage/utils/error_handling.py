import requests
import time
import logging
from typing import Callable, Optional

class RequestRetryHandler:
    """
    A helper class to wrap API calls with retry logic and session reuse.
    """

    def __init__(self, max_retries: int = 5, backoff_factor: float = 0.5, status_forcelist: Optional[list] = None):
        """
        Initialize the retry handler.

        Args:
            max_retries (int): Maximum number of retry attempts.
            backoff_factor (float): Backoff factor for exponential delay.
            status_forcelist (list): HTTP status codes to trigger retry.
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist or [429, 500, 502, 503, 504]
        self.session = requests.Session()

    def request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Perform a request with retry logic.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST').
            url (str): The URL for the request.
            **kwargs: Additional arguments passed to `requests.request`.

        Returns:
            requests.Response: Response object.

        Raises:
            requests.HTTPError: If all retries fail.
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                if response.status_code in self.status_forcelist:
                    raise requests.HTTPError(f"Status {response.status_code}", response=response)
                return response
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
                wait_time = self.backoff_factor * (2 ** attempt)
                logging.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {wait_time:.1f}s.")
                time.sleep(wait_time)
        raise requests.HTTPError(f"All {self.max_retries} retry attempts failed.")
