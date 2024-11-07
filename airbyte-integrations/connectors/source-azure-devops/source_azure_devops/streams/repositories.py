from typing import Any, Mapping
from airbyte_cdk.models import SyncMode
from source_azure_devops.streams.azure_devops_stream import AzureDevopsSubStream, AzureDevopsStream

class Repositories(AzureDevopsSubStream):
    """
    Stream returning Repositories from Azure Devops REST API.  
    Documentation: https://docs.microsoft.com/en-us/rest/api/azure/devops/git/repositories/list
    """

    primary_key = "id"
    api_version = "7.1"

    def __init__(self, parent: AzureDevopsStream, config: Mapping[str, Any], authenticator=None, api_budget=None):
        super().__init__(parent, config, authenticator, api_budget)

    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return f"{stream_slice['name']}/_apis/git/repositories"
    
    def stream_slices(self, sync_mode, cursor_field = None, stream_state = None):
        for parent_record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield parent_record

    def parse_response(self, response, stream_slice: dict, **kwargs):
        for repository in response.json().get('value', []):
            # Removing project object and adding projectId
            repository['projectId'] = stream_slice['id']
            del repository['project']
            
            yield repository