# Basic full refresh stream
from abc import ABC
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
import requests
from typing import Any, Iterable, Mapping, MutableMapping, Optional

class AzureDevopsStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class AzureDevopsStream(HttpStream, ABC)` which is the current class
    `class Customers(AzureDevopsStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(AzureDevopsStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalAzureDevopsStream((AzureDevopsStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """
    api_version: str = "7.1"
    subdomain: Optional[str] = None  # Some endpoints are part of subdomains, e.g: https://vssps.dev.azure.com/
    url_base: str = None

    def __init__(self, config: Mapping[str, Any], authenticator: HttpAuthenticator, api_budget = None):
        if self.subdomain is None:
            self.url_base = f"https://dev.azure.com/{config['organization']}/"
        else: 
            self.url_base = f"https://{self.subdomain}.dev.azure.com/{config['organization']}/"
        super().__init__(authenticator=authenticator, api_budget=api_budget)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "api-version": self.api_version
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json().get('value', [])

class AzureDevopsSubStream(HttpSubStream, ABC):

    api_version: str = "7.1"
    subdomain: Optional[str] = None  # Some endpoints are part of subdomains, e.g: https://vssps.dev.azure.com/
    url_base: str = None

    def __init__(self, parent: AzureDevopsStream, config: Mapping[str, Any], authenticator = None, api_budget = None):
        if self.subdomain is None:
            self.url_base = f"https://dev.azure.com/{config['organization']}/"
        else:
            self.url_base = f"https://{self.subdomain}.dev.azure.com/{config['organization']}/"
        super().__init__(parent=parent, authenticator=authenticator, api_budget=api_budget)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "api-version": self.api_version
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json().get('value', [])

    def stream_slices(self, sync_mode, cursor_field = None, stream_state = None):
        return super().stream_slices(sync_mode, cursor_field, stream_state)



# Basic incremental stream
class IncrementalAzureDevopsStream(AzureDevopsStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}