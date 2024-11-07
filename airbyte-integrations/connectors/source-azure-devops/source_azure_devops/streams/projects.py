from typing import Any, Mapping, Optional

from source_azure_devops.streams.azure_devops_stream import AzureDevopsStream

class Projects(AzureDevopsStream):
    """
    Stream returning Projects from Azure Devops REST API.  
    Documentation: https://docs.microsoft.com/en-us/rest/api/azure/devops/core/projects/list
    """

    primary_key = "id"

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "_apis/projects"
    
    @property
    def use_cache(self) -> bool:
        """
        Cached to be used by substreams such as Repositories
        """
        return True
