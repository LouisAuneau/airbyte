from typing import Any, Mapping, Optional

from source_azure_devops.streams.azure_devops_stream import AzureDevopsStream

class Users(AzureDevopsStream):
    """
    Stream returning Users from Azure Devops REST API.  
    Documentation: https://learn.microsoft.com/en-us/rest/api/azure/devops/graph/users/list
    """

    primary_key = "descriptor"
    api_version = "7.1-preview.1"
    subdomain = "vssps"

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "_apis/graph/users"
