from typing import Any, List, Mapping, Optional
from airbyte_cdk.models import SyncMode
from source_azure_devops.streams.azure_devops_stream import AzureDevopsSubStream

class WorkItemTypes(AzureDevopsSubStream):
    """
    Stream returning Work Item Types from Azure Devops REST API.
    Documentation: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-item-types/list
    """
    primary_key = "name"
    api_version = "7.1"
    
    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return f"{stream_slice['name']}/_apis/wit/workitemtypes"
    
    def stream_slices(self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None):
        for parent_record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield parent_record