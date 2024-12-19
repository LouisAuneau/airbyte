import logging
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
    WorkItems
)


class SourceAzureDevops(AbstractSource):
    """
    Azure DevOps Source
    """

    def check_connection(self, logger: logging.Logger, config: MutableMapping[str, Any]) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

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
        work_items_stream = WorkItems(config=config, authenticator=auth)

        return [
            boards_stream,
            project_stream,
            pull_requests_stream,
            repositories_stream,
            teams_stream,
            users_stream,
            work_items_stream
        ]
