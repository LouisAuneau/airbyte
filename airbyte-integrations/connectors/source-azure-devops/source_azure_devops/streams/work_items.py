from typing import Iterable, List, Mapping, Any
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.streams import IncrementalMixin
from source_azure_devops.streams.azure_devops_stream import AzureDevopsStream

class WorkItems(AzureDevopsStream, IncrementalMixin):
    """
    Stream returning Work Items from Azure Devops REST API.
    Documentation: 
        - https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/wiql/query-by-wiql
        - https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-items/get-work-items-batch

    This stream is special because it uses the WIQL endpoint to retrieve the latest updated work items IDs, 
    and then the Work Items Batch endpoint to retrieve the work items details and thus state.
    """

    primary_key = "id"
    api_version = "7.1"
    http_method = "POST"
    cursor_field = 'updatedAt'

    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return "_apis/wit/wiql"
    
    def request_params(self, stream_state, stream_slice = None, next_page_token = None):
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params['timePrecision'] = 'true'
        return params

    def request_body_json(self, stream_state, stream_slice = None, next_page_token = None):
        if self.cursor_field in stream_state:
            return {
                "query": f"Select [System.Id] From WorkItems Where [System.ChangedDate] > '{stream_state[self.cursor_field]}' order by [System.ChangedDate] asc"
            }
        else:
            return {
                "query": "Select [System.Id] From WorkItems order by [System.ChangedDate] asc"
            }

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value:
            self._state = value

    def read_records(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        )
        for record in records:
            yield record
            self.state = {
                self.cursor_field: record['fields']['System.ChangedDate']
            }

    def parse_response(self, response, **kwargs):
        """
        From the list of updated work items IDs fetched using WIQL endpoint,
        it fetches work items details using the get work items batch endpoint.
        Finally, it denormalize some work items fields, adds the update cursor field and yields them.

        Args:
            response (requests.Response): The response object returned from the initial WIQL API call.
            **kwargs: Additional keyword arguments.

        Yields:
            dict: A dictionary representing a processed work item.

        Raises:
            AirbyteTracedException: If the batch API call returns a non-200 status code.
        """
        work_items = response.json().get('workItems', [])
        batches = [work_items[i:i + 200] for i in range(0, len(work_items), 200)]  # Batching work items by 200

        for batch in batches:
            work_item_ids = [wi['id'] for wi in batch]
            url = f"{self.url_base}_apis/wit/workitemsbatch"
            
            response = self._session.post(
                url,
                params={"api-version": self.api_version},
                json={"ids": work_item_ids}
            )

            if response.status_code != 200:
                raise AirbyteTracedException(
                    internal_message=(f"Stream: `{self.name}`. HTTP error during Work Items Batch API call."),
                    message=response.text,
                    failure_type=FailureType.transient_error
                )

            work_items = response.json().get('value', [])
            for work_item in work_items:
                work_item['updatedAt'] = work_item['fields']['System.ChangedDate']  # Cursor field
                work_item['fields']['System.CreatedBy'] =  work_item['fields']['System.CreatedBy']['descriptor']
                if 'System.AssignedTo' in work_item['fields']:
                    work_item['fields']['System.AssignedTo'] = work_item['fields']['System.AssignedTo']['descriptor']
                if 'System.ChangedBy' in work_item['fields']:
                    work_item['fields']['System.ChangedBy'] = work_item['fields']['System.ChangedBy']['descriptor']
                if 'System.Tags' in work_item['fields']:
                    work_item['fields']['System.Tags'] = work_item['fields']['System.Tags'].split(";")
                yield work_item