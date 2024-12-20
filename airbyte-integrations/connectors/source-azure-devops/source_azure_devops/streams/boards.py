from typing import Any, Iterable, List, Mapping, Optional
from airbyte_cdk.models import SyncMode
import requests

from source_azure_devops.streams.azure_devops_stream import AzureDevopsSubStream

class Boards(AzureDevopsSubStream):
    """
    Stream returning Boards from Azure Devops REST API.  
    Documentation: https://learn.microsoft.com/en-us/rest/api/azure/devops/work/boards/list
    """

    primary_key = "id"
    api_version = "7.1"

    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return f"{stream_slice['projectName']}/{stream_slice['name']}/_apis/work/boards"
    
    def stream_slices(self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None):
        for parent_record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield parent_record

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        for response in response.json().get('value', []):
            response['projectId'] = stream_slice['projectId']
            response['teamId'] = stream_slice['id']
            yield response
        