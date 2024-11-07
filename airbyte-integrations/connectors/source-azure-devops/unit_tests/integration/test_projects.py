# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timezone
import json
from typing import Any, Dict, Mapping, Optional
from unittest import TestCase
import uuid

import freezegun
from airbyte_cdk.sources.source import TState
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_protocol.models import ConfiguredAirbyteCatalog, SyncMode
from source_azure_devops import SourceAzureDevops

_A_CONFIG = {
	"personal_access_token": "test_token",
    "organization": "airbyte-test"
}
_NOW = datetime.now(timezone.utc)

@freezegun.freeze_time(_NOW.isoformat())
class FullRefreshTest(TestCase):

    @HttpMocker()
    def test_read_a_single_page(self, http_mocker: HttpMocker) -> None:
        http_mocker.get(
            HttpRequest(url="https://dev.azure.com/airbyte-test/_apis/projects?api-version=7.1"),
            HttpResponse(
                body=json.dumps({
                    "count": 2,
                    "value": [
                        {
                            "id": str(uuid.uuid4()),
                            "name": "Airbyte Test Project 1",
                            "url": f"https://dev.azure.com/airbyte-test/_apis/projects/{str(uuid.uuid4())}",
                            "state": "wellFormed",
                            "revision": 123,
                            "visibility": "private",
                            "lastUpdateTime": "2021-01-01T01:02:03.123Z"
                        },
                        {
                            "id": str(uuid.uuid4()),
                            "name": "Airbyte Test Project 2",
                            "url": f"https://dev.azure.com/inarix/_apis/airbyte-test/{str(uuid.uuid4())}",
                            "state": "wellFormed",
                            "revision": 123,
                            "visibility": "public",
                            "lastUpdateTime": "2021-01-01T01:02:03.123Z"
                        }
                    ]
                }),
                status_code=200
            )
        )

        output = self._read(_A_CONFIG, _configured_catalog("projects", SyncMode.full_refresh))
        assert len(output.records) == 2

    def _read(self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, expecting_exception: bool = False) -> EntrypointOutput:
        return _read(config, configured_catalog=configured_catalog, expecting_exception=expecting_exception)

def _read(
    config: Mapping[str, Any],
    configured_catalog: ConfiguredAirbyteCatalog,
    state: Optional[Dict[str, Any]] = None,
    expecting_exception: bool = False
) -> EntrypointOutput:
    return read(_source(configured_catalog, config, state), config, configured_catalog, state, expecting_exception)


def _configured_catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Dict[str, Any], state: Optional[TState]) -> SourceAzureDevops:
    return SourceAzureDevops()