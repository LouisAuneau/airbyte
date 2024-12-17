from airbyte_cdk.models import SyncMode

from source_azure_devops.streams.azure_devops_stream import AzureDevopsSubStream
from source_azure_devops.streams.paginators import AzureDevOpsPaginator
from source_azure_devops.utils import round_datetimes_ms

class PullRequests(AzureDevopsSubStream):
    """
    Stream returning Pull Requests from Azure Devops REST API.  
    Documentation: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-requests/get-pull-requests
    """

    primary_key = ["pullRequestId", "repositoryId"]
    api_version = "7.1"
    paginator = AzureDevOpsPaginator.TOP_SKIP

    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return f"_apis/git/repositories/{stream_slice['id']}/pullrequests"

    def stream_slices(self, sync_mode, cursor_field = None, stream_state = None):
        parent_slices = self.parent.stream_slices(sync_mode=SyncMode.full_refresh)
        for parent_slice in parent_slices:
            for parent_record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=parent_slice):
                yield parent_record

    def request_params(self, stream_state, stream_slice = None, next_page_token = None):
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params["searchCriteria.status"] = "all"
        return params

    def parse_response(self, response, **kwargs):
        for pr in response.json().get('value', []):
            pr['repositoryId'] = pr['repository']['id']
            pr['createdBy'] = pr['createdBy']['descriptor']
            pr['lastMergeSourceCommit'] = pr.get('lastMergeSourceCommit', {}).get('commitId', None)
            pr['lastMergeTargetCommit'] = pr.get('lastMergeTargetCommit', {}).get('commitId', None)
            pr['lastMergeCommit'] = pr.get('lastMergeCommit', {}).get('commitId', None)
            pr = round_datetimes_ms(pr, self.get_json_schema())
            del pr['repository']

            yield pr