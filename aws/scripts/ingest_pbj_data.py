"""
HealthcareMetricsProject - Bronze Ingestion Job (AWS Glue Python Shell)

Purpose
-------
Incrementally pulls new/changed CMS PBJ files from a shared Google Drive
folder and lands them in the Bronze S3 bucket, using the Google Drive
Changes API so we only ever process what's new since the last run.

Flow
----
1. Read the last saved Drive "page token" (the sync cursor) from DynamoDB.
   If there isn't one yet, this is treated as a first run and a fresh
   starting token is requested from Google Drive.
2. Load the Google service account credentials from Secrets Manager and
   build an authenticated Drive API client.
3. Ask Drive for every change since that token, and keep paging until
   Drive says there's nothing left.
4. Of those changes, keep only files that (a) live in our target Drive
   folder, (b) aren't folders themselves, and (c) haven't been removed.
5. Download each qualifying file and upload it to the Bronze bucket.
6. Only after every file uploads successfully, save the new page token
   back to DynamoDB so the next run picks up from here.

If anything raises an exception before step 6, the cursor is deliberately
NOT updated, so a failed run is retried from the same starting point next
time rather than silently skipping files. The exception is left to
propagate so the Glue job run is marked FAILED, which is what triggers
the SNS failure alert already wired up via EventBridge.

Required Glue job parameters (set via --additional-python-modules and
regular job arguments, not hardcoded here):
    --DRIVE_FOLDER_ID     Google Drive folder ID to watch
    --BRONZE_BUCKET       S3 bucket name for the Bronze zone
    --SECRET_NAME         Secrets Manager secret holding the service
                           account JSON key
    --DYNAMODB_TABLE      DynamoDB table holding the sync cursor
    --AWS_REGION          AWS region (e.g. us-west-1)

Required extra Python libraries (set via the Glue job's
--additional-python-modules parameter, comma-separated, no spaces):
    google-api-python-client,google-auth,google-auth-httplib2
"""

import io
import json
import os
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
STATE_ID = "google_drive_sync"
DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

CHANGES_FIELDS = (
    "nextPageToken,newStartPageToken,"
    "changes(fileId,removed,file(id,name,mimeType,parents,modifiedTime,trashed))"
)

# CMS PBJ files ship with the month/date baked into the filename (e.g.
# "NH_CitationDescriptions_Oct2024.csv") and, for a couple of files, a
# fiscal year baked into the front instead (e.g.
# "FY_2024_SNF_VBP_Facility_Performance.csv"). Stripping that out gives
# a stable dataset identifier so the same conceptual dataset lands in
# ONE Glue table over time (partitioned by ingestion_date), instead of
# a brand new table every time CMS ships next month's file.
FISCAL_YEAR_PREFIX_PATTERN = re.compile(r"^FY_\d{4}_", re.IGNORECASE)
DATE_SUFFIX_PATTERN = re.compile(
    r"(_(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\d{4}|_\d{8})$",
    re.IGNORECASE,
)


def get_job_args():
    """Read the job parameters Glue was started with."""
    return getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DRIVE_FOLDER_ID",
            "BRONZE_BUCKET",
            "SECRET_NAME",
            "DYNAMODB_TABLE",
            "AWS_REGION",
        ],
    )


def get_drive_service(secret_name, region):
    """Build an authenticated Google Drive API client from the service
    account key stored in Secrets Manager."""
    secrets_client = boto3.client("secretsmanager", region_name=region)
    secret_value = secrets_client.get_secret_value(SecretId=secret_name)
    service_account_info = json.loads(secret_value["SecretString"])
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def get_saved_page_token(dynamodb_resource, table_name):
    """Return the last saved page token, or None if this is the first run."""
    table = dynamodb_resource.Table(table_name)
    response = table.get_item(Key={"state_id": STATE_ID})
    item = response.get("Item")
    if item is None:
        return None
    return item.get("page_token")


def save_page_token(dynamodb_resource, table_name, page_token):
    """Persist the new page token so the next run starts from here."""
    table = dynamodb_resource.Table(table_name)
    table.put_item(
        Item={
            "state_id": STATE_ID,
            "page_token": page_token,
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


def list_existing_files_in_folder(drive_service, folder_id):
    """List every non-folder, non-trashed file currently sitting in the
    target Drive folder. Used only once, during the initial backfill on
    the very first run - the Changes API has no visibility into files
    that already existed before the starting bookmark was created."""
    files = []
    page_token = None
    query = (
        f"'{folder_id}' in parents "
        f"and trashed = false "
        f"and mimeType != '{DRIVE_FOLDER_MIME_TYPE}'"
    )

    while True:
        response = (
            drive_service.files()
            .list(q=query, fields="nextPageToken, files(id, name)", pageSize=100, pageToken=page_token)
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if page_token is None:
            break

    return files


def fetch_all_changes(drive_service, start_page_token):
    """Page through the Drive Changes API from start_page_token until
    there's nothing left. Returns (list_of_change_records, new_page_token)."""
    changes = []
    page_token = start_page_token
    new_start_page_token = None

    while page_token is not None:
        response = (
            drive_service.changes()
            .list(pageToken=page_token, fields=CHANGES_FIELDS, pageSize=100)
            .execute()
        )
        changes.extend(response.get("changes", []))

        if "newStartPageToken" in response:
            new_start_page_token = response["newStartPageToken"]

        page_token = response.get("nextPageToken")

    return changes, new_start_page_token


def is_relevant_file(change, target_folder_id):
    """Keep only changes that are: not removed, not a folder, and
    actually located inside our target Drive folder."""
    if change.get("removed"):
        return False

    file_info = change.get("file")
    if file_info is None:
        return False

    if file_info.get("trashed"):
        return False

    if file_info.get("mimeType") == DRIVE_FOLDER_MIME_TYPE:
        return False

    parents = file_info.get("parents", [])
    return target_folder_id in parents


def download_file_bytes(drive_service, file_id):
    """Download a Drive file's raw bytes into memory."""
    request = drive_service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer.read()


def derive_dataset_name(file_name):
    """Strip the month/date or fiscal-year portion out of a raw CMS
    filename to get a stable dataset identifier.

    Examples:
        NH_CitationDescriptions_Oct2024.csv       -> NH_CitationDescriptions
        NH_CovidVaxAverages_20241027.csv          -> NH_CovidVaxAverages
        FY_2024_SNF_VBP_Facility_Performance.csv  -> SNF_VBP_Facility_Performance
        Test_file_1.csv                           -> Test_file_1 (unchanged - no
                                                      date/fiscal-year pattern found)
    """
    name_without_ext, _ext = os.path.splitext(file_name)
    name_without_ext = FISCAL_YEAR_PREFIX_PATTERN.sub("", name_without_ext)
    name_without_ext = DATE_SUFFIX_PATTERN.sub("", name_without_ext)
    return name_without_ext


def upload_to_bronze(s3_client, bucket_name, file_name, file_bytes):
    """Land a downloaded file in the Bronze bucket, grouped by a stable
    dataset name and partitioned by the date it was ingested."""
    dataset_name = derive_dataset_name(file_name)
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/dataset={dataset_name}/ingestion_date={ingestion_date}/{file_name}"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=file_bytes)
    return key


def main():
    args = get_job_args()
    region = args["AWS_REGION"]

    print(f"Starting ingestion job: {args['JOB_NAME']}")

    drive_service = get_drive_service(args["SECRET_NAME"], region)
    dynamodb_resource = boto3.resource("dynamodb", region_name=region)
    s3_client = boto3.client("s3", region_name=region)

    saved_token = get_saved_page_token(dynamodb_resource, args["DYNAMODB_TABLE"])

    if saved_token is None:
        print("No saved cursor found - treating this as the first run.")
        print("Backfilling every file already sitting in the target folder...")

        existing_files = list_existing_files_in_folder(drive_service, args["DRIVE_FOLDER_ID"])
        print(f"Found {len(existing_files)} existing file(s) to backfill.")

        for file_info in existing_files:
            file_id = file_info["id"]
            file_name = file_info["name"]

            print(f"Downloading '{file_name}' ({file_id})...")
            file_bytes = download_file_bytes(drive_service, file_id)

            s3_key = upload_to_bronze(s3_client, args["BRONZE_BUCKET"], file_name, file_bytes)
            print(f"Uploaded to s3://{args['BRONZE_BUCKET']}/{s3_key}")

        # Only mint and save the starting bookmark AFTER the backfill
        # succeeds - if the backfill fails partway through, saved_token
        # is still None next run, so it retries the whole backfill
        # rather than silently starting incremental sync from a point
        # that skips whatever didn't finish uploading.
        start_token_response = drive_service.changes().getStartPageToken().execute()
        saved_token = start_token_response["startPageToken"]
        save_page_token(dynamodb_resource, args["DYNAMODB_TABLE"], saved_token)
        print(f"Backfill complete. Saved initial page token: {saved_token}")
        return

    print(f"Resuming from saved page token: {saved_token}")
    changes, new_page_token = fetch_all_changes(drive_service, saved_token)
    print(f"Retrieved {len(changes)} raw change record(s) from Drive.")

    relevant_changes = [
        change for change in changes if is_relevant_file(change, args["DRIVE_FOLDER_ID"])
    ]
    print(f"{len(relevant_changes)} change(s) apply to the target folder.")

    uploaded_count = 0
    for change in relevant_changes:
        file_info = change["file"]
        file_id = file_info["id"]
        file_name = file_info["name"]

        print(f"Downloading '{file_name}' ({file_id})...")
        file_bytes = download_file_bytes(drive_service, file_id)

        s3_key = upload_to_bronze(s3_client, args["BRONZE_BUCKET"], file_name, file_bytes)
        print(f"Uploaded to s3://{args['BRONZE_BUCKET']}/{s3_key}")
        uploaded_count += 1

    # Only advance the cursor once every file above has uploaded without
    # raising - if something failed partway through, we want the next
    # run to start from the OLD token and retry, not skip ahead.
    if new_page_token is not None:
        save_page_token(dynamodb_resource, args["DYNAMODB_TABLE"], new_page_token)
        print(f"Saved new page token: {new_page_token}")

    print(f"Done. {uploaded_count} file(s) ingested into Bronze.")


if __name__ == "__main__":
    main()