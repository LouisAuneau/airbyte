#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_azure_devops import SourceAzureDevops

if __name__ == "__main__":
    source = SourceAzureDevops()
    launch(source, sys.argv[1:])
