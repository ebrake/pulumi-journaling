#!/usr/bin/env python3
"""Initialize isolated storage fixtures and run the journaling backend tests.

Requires boto3 and azure-storage-blob in the test environment. All endpoints
are loopback addresses; the credentials are public emulator fixtures.
"""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request

import boto3
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

AZURE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:19000",
        aws_access_key_id="journaltest",
        aws_secret_access_key="journaltestpassword",
        region_name="us-east-1",
    )
    for attempt in range(30):
        try:
            s3.list_buckets()
            break
        except s3.exceptions.ClientError:
            raise
        except Exception:
            if attempt == 29:
                raise
            time.sleep(1)
    s3.create_bucket(Bucket="journaltest")
    azure = BlobServiceClient(
        "http://127.0.0.1:11000/devstoreaccount1", credential=AZURE_KEY
    )
    try:
        azure.create_container("journaltest")
    except ResourceExistsError:
        pass
    data = json.dumps({"name": "journaltest"}).encode()
    request = urllib.request.Request(
        "http://127.0.0.1:14443/storage/v1/b",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
    environment = dict(
        os.environ,
        PULUMI_JOURNAL_TEST_GS_URL="gs://journaltest/journal-canary/storage",
        PULUMI_JOURNAL_TEST_S3_URL="s3://journaltest/journal-canary/storage?endpoint=http%3A%2F%2F127.0.0.1%3A19000&region=us-east-1&use_path_style=true",
        PULUMI_JOURNAL_TEST_AZBLOB_URL="azblob://journaltest/journal-canary/storage?storage_account=devstoreaccount1&domain=127.0.0.1:11000&protocol=http&localemu=true",
        PULUMI_JOURNAL_TEST_POSTGRES_URL="postgres://journaltest:journaltestpassword@127.0.0.1:15432/journaltest?sslmode=disable&table=journal_canary_",
        STORAGE_EMULATOR_HOST="127.0.0.1:14443",
        GOOGLE_OAUTH_ACCESS_TOKEN="emulator",
        AWS_ACCESS_KEY_ID="journaltest",
        AWS_SECRET_ACCESS_KEY="journaltestpassword",
        AWS_SESSION_TOKEN="",
        AWS_EC2_METADATA_DISABLED="true",
        AZURE_STORAGE_ACCOUNT="devstoreaccount1",
        AZURE_STORAGE_KEY=AZURE_KEY,
    )
    subprocess.run(
        [
            "go",
            "-C",
            "pkg",
            "test",
            "-race",
            "-count=1",
            "-v",
            "./backend/diy",
            "./backend/diy/postgres",
        ],
        env=environment,
        check=True,
    )


if __name__ == "__main__":
    main()
