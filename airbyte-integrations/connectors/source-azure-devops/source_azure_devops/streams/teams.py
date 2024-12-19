from source_azure_devops.streams.azure_devops_stream import AzureDevopsStream
from source_azure_devops.streams.paginators import AzureDevOpsPaginator


class Teams(AzureDevopsStream):
    """
    Stream returning Teams from Azure Devops REST API.  
    Documentation: https://learn.microsoft.com/en-us/rest/api/azure/devops/core/teams
    """

    primary_key = "id"
    api_version = "7.1-preview.3"
    paginator = AzureDevOpsPaginator.TOP_SKIP
    
    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return "_apis/teams"
    
    @property
    def use_cache(self) -> bool:
        """
        Cached to be used by substreams
        """
        return True