from dataclasses import dataclass
from typing import Iterable

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import SubstreamPartitionRouter
from airbyte_cdk.sources.types import StreamSlice

@dataclass
class ProjectsNestedPartitionRouter(SubstreamPartitionRouter):
    """
    Custom partition router that produce partitions for nested streams with a parent stream already sliced by project name.

    Usage:
    ```
    partition_router:
        type: CustomPartitionRouter
        class_name: source_azure_devops.partition_routers.ProjectsNestedPartitionRouter
        parent_stream_configs:
        - type: ParentStreamConfig
            parent_key: "<PARENT_KEY>"
            stream: "#/definitions/streams/<PARENT_STREAM>"
            partition_field: "<PARTITION_NAME>"
    ```

    Produces slices like:
    ```
    {
        "project_name": "...",
        "<PARTITION_NAME>": "..."
    }
    ```
    """

    def stream_slices(self) -> Iterable[StreamSlice]:
        parent_stream = self.parent_stream_configs[0]

        for stream_slice in parent_stream.stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in parent_stream.stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice):
                yield StreamSlice(partition={
                    parent_stream.partition_field.string: record[parent_stream.parent_key.string],
                    "project_name": stream_slice["project_name"]
                }, cursor_slice={})