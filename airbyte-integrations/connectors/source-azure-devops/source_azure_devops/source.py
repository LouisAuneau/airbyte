import logging
import requests
from typing import Any, List, Mapping, MutableMapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator

from source_azure_devops.streams import (
    Boards,
    Projects,
    PullRequests,
    Repositories,
    Teams,
    Users,
    WorkItems,
    WorkItemTypes
)


class SourceAzureDevops(AbstractSource):
    """
    Azure DevOps Source
    """

    def check_connection(self, logger: logging.Logger, config: MutableMapping[str, Any]) -> Tuple[bool, any]:
        """
        Connection check to validate that the user-provided config can be used to connect to Azure DevOps API

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        res = requests.get(
            url=f"https://dev.azure.com/{config['organization']}/_apis/projects", 
            auth=BasicHttpAuthenticator(username="", password=config["personal_access_token"]), 
            timeout=10,
            allow_redirects=False
        )
        if res.status_code == 200:
            return True, None
        elif res.status_code == 302 and "vssps.visualstudio.com/_signin" in res.headers.get("Location", ""):
            return False, "Unauthorized: Personal Access Token is invalid."
        elif res.status_code == 401:
            return False, f"Unauthorized: Unauthorized Personal Access Token for organization `{config['organization']}`."
        elif res.status_code == 404:
            return False, f"Not Found: Organization `{config['organization']}` not found."
        return False, f"Unexpected error: Failed with status code {res.status_code} and message {res.text}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        List all streams supported by Azure DevOps source connector.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :return List[Stream]: A list of stream objects
        """
        auth = BasicHttpAuthenticator(username="", password=config["personal_access_token"])

        project_stream = Projects(config=config, authenticator=auth)
        users_stream = Users(config=config, authenticator=auth)
        teams_stream = Teams(config=config, authenticator=auth)
        boards_stream = Boards(parent=teams_stream, config=config, authenticator=auth)
        repositories_stream = Repositories(parent=project_stream, config=config, authenticator=auth)
        pull_requests_stream = PullRequests(parent=repositories_stream, config=config, authenticator=auth)
        work_item_types_stream = WorkItemTypes(parent=project_stream, config=config, authenticator=auth)
        work_items_stream = WorkItems(config=config, authenticator=auth)

        return [
            boards_stream,
            project_stream,
            pull_requests_stream,
            repositories_stream,
            teams_stream,
            users_stream,
            work_item_types_stream,
            work_items_stream
        ]
