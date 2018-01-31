#!/usr/bin/env python
import json
import logging
import sys
import time
import typing
import uuid

import boto3
import os
import requests
from boto3.s3.transfer import TransferConfig
from cloud_blobstore import BlobStore, s3
from hca.dss import DSSClient
from hca.dss.upload_to_cloud import _mime_type, encode_tags, upload_to_cloud
from hca.util import SwaggerAPIException
from io import open

from packages.checksumming_io.checksumming_io import ChecksummingSink, S3Etag

"""
Utility to load data bundles in AWS S3 into the HCA Data Storage System (DSS).
"""

logger = logging.getLogger(__name__)

DSS_ENDPOINT = "https://commons-dss.ucsc-cgp-dev.org/v1"

SCHEMA_URL = ("https://raw.githubusercontent.com/DataBiosphere/commons-sample-data/master"
              "/json_schema/spinnaker_metadata/1.1.1/spinnaker_metadata_schema.json")
SCHEMA_VERSION = "1.1.1"
SCHEMA_TYPE = "spinnaker_metadata"

BUCKET = "cgp-commons-public"
STAGING_BUCKET = "commons-dss-staging"

BUNDLES_PATH = "topmed_open_access"

CREATOR_ID = 20


class DssUploader:
    def __init__(self, dss_endpoint: str, staging_bucket: str) -> None:
        self.dss_endpoint = dss_endpoint
        self.staging_bucket = staging_bucket
        self.s3_client = boto3.client("s3")
        self.blobstore = s3.S3BlobStore(self.s3_client)
        os.environ.pop('HCA_CONFIG_FILE', None)
        self.dss_client = DSSClient()
        self.dss_client.host = "https://commons-dss.ucsc-cgp-dev.org/v1"

    def upload_cloud_file(self, bucket, key, bundle_uuid, file_uuid) -> tuple:
        if not self._has_hca_tags(self.blobstore, bucket, key):
            checksums = self._calculate_checksums(self.s3_client, bucket, key)
            self._set_hca_metadata_tags(self.s3_client, bucket, key, checksums)
        return self._upload_tagged_cloud_file_to_dss(bucket, key, file_uuid, bundle_uuid)

    def upload_local_file(self, path, bundle_uuid: str):
        file_uuid, key = self._upload_local_file_to_staging(path)
        return self._upload_tagged_cloud_file_to_dss(self.staging_bucket, key, file_uuid, bundle_uuid)

    @staticmethod
    def get_filename_from_key(key: str):
        return key.split("/")[-1]

    def _upload_local_file_to_staging(self, path: str):
        with open(path, "rb") as fh:
            file_uuids, key_names = upload_to_cloud([fh], self.staging_bucket, "aws")
        return file_uuids[0], key_names[0]

    @staticmethod
    def _has_hca_tags(blobstore: BlobStore, bucket: str, key: str) -> bool:
        hca_tag_names = {"hca-dss-s3_etag", "hca-dss-sha1", "hca-dss-sha256", "hca-dss-crc32c"}
        metadata = blobstore.get_user_metadata(bucket, key)
        return hca_tag_names.issubset(metadata.keys())

    @staticmethod
    def _calculate_checksums(s3_client, bucket: str, key: str) -> typing.Dict:
        checksumming_sink = ChecksummingSink()
        tx_cfg = TransferConfig(multipart_threshold=S3Etag.etag_stride,
                                multipart_chunksize=S3Etag.etag_stride)
        s3_client.download_fileobj(bucket, key, checksumming_sink, Config=tx_cfg)
        return checksumming_sink.get_checksums()

    def _get_content_type_from_name(self, key: str) -> str:
        return _mime_type(self.get_filename_from_key(key))

    def _set_hca_metadata_tags(self, s3_client, bucket: str, key: str, checksums: dict) -> None:
        metadata = {
            "hca-dss-s3_etag": checksums["s3_etag"],
            "hca-dss-sha1": checksums["sha1"],
            "hca-dss-sha256": checksums["sha256"],
            "hca-dss-crc32c": checksums["crc32c"]
        }
        s3_client.put_object_tagging(Bucket=bucket,
                                     Key=key,
                                     Tagging=dict(TagSet=encode_tags(metadata))
                                     )

    def _upload_tagged_cloud_file_to_dss(self, source_bucket: str, source_key: str, file_uuid: str, bundle_uuid: str,
                                         timeout_seconds=1200):
        source_url = f"s3://{source_bucket}/{source_key}"

        response = self.dss_client.put_file._request(dict(
            uuid=file_uuid,
            bundle_uuid=bundle_uuid,
            creator_uid=CREATOR_ID,
            source_url=source_url
        ))
        file_version = response.json().get('version', "blank")
        # files_uploaded.append(dict(name=filename, version=version, uuid=file_uuid, creator_uid=creator_uid))

        if response.status_code in (requests.codes.ok, requests.codes.created):
            logger.info("File %s: Sync copy -> %s", source_url, file_version)
        else:
            assert response.status_code == requests.codes.accepted
            logger.info("File %s: Async copy -> %s", source_url, file_version)

            timeout = time.time() + timeout_seconds
            wait = 1.0
            while time.time() < timeout:
                try:
                    self.dss_client.head_file(uuid=file_uuid, replica="aws", version=file_version)
                    break
                except SwaggerAPIException as e:
                    if e.code != requests.codes.not_found:
                        msg = "File {}: Unexpected server response during registration"
                        raise RuntimeError(msg.format(source_url))
                    time.sleep(wait)
                    wait = min(60.0, wait * self.dss_client.UPLOAD_BACKOFF_FACTOR)
            else:
                # timed out. :(
                raise RuntimeError("File {}: registration FAILED".format(source_url))
            logger.debug("Successfully uploaded file")

        file_name = self.get_filename_from_key(source_key)
        return file_uuid, file_version, file_name


class MetadataFileUploader:
    def __init__(self, dss_uploader: DssUploader) -> None:
        self.dss_uploader = dss_uploader

    def load_file(self, bucket: str, key: str, filename: str, schema_url: str, schema_version: str, schema_type: str,
                  bundle_uuid: str) -> tuple:
        metadata_string = self.dss_uploader.blobstore.get(bucket, key).decode("utf-8")
        metadata = json.loads(metadata_string)
        metadata['core'] = dict(schema_url=schema_url, schema_version="1.1.1", type="metadata")
        file_path = "/tmp/" + filename
        with open(file_path, "w") as fh:
            fh.write(json.dumps(metadata, indent=4))
        return self.dss_uploader.upload_local_file(file_path, bundle_uuid)


class BundleUploader:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    def load_bundle(self, bucket, bundle_key, bundle_uuid):
        file_info_list = self._load_bundle_files(bucket, bundle_key, bundle_uuid)
        return self._load_bundle(file_info_list, bundle_uuid)

    def load_all_bundles(self, bucket, bundles_key):
        count = 0
        for key in self.dss_uploader.blobstore.list(bucket, bundles_key):
            if key.endswith("/metadata.json"):
                bundle_key = "/".join(key.split("/")[:-1])
                bundle_uuid = bundle_key.split("/")[-1]
                logger.info(f"Loading bundle {count} at: s3://{bucket}/{bundle_key}")
                self.load_bundle(bucket, bundle_key, bundle_uuid)
                count += 1
        logger.info(f"Loaded {count} bundles")

    def _load_bundle_files(self, bucket: str, bundle_key: str, bundle_uuid: str) -> typing.List[typing.Dict[str, str]]:
        # List the files in bundle_key
        file_info_list = []
        for file_key in self.dss_uploader.blobstore.list(bucket, bundle_key):
            filename = DssUploader.get_filename_from_key(file_key)
            if filename == "":
                continue
            elif filename == "metadata.json":
                file_uuid, file_version, filename = \
                    self.metadata_file_uploader.load_file(bucket, file_key, filename,
                                                          SCHEMA_URL, SCHEMA_VERSION, SCHEMA_TYPE,
                                                          bundle_uuid)
                file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=True))
            else:
                file_uuid = str(uuid.uuid4())
                file_uuid, file_version, filename = \
                    self.dss_uploader.upload_cloud_file(bucket, file_key, bundle_uuid, file_uuid)
                file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))
        return file_info_list

    def _load_bundle(self, file_info_list: list, bundle_uuid: str):
        response = self.dss_uploader.dss_client.put_bundle(replica="aws", creator_uid=CREATOR_ID, files=file_info_list,
                                                           uuid=bundle_uuid)
        version = response['version']
        return f"{bundle_uuid}.{version}"


def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def main(args):
    dss_endpoint = DSS_ENDPOINT
    bucket = BUCKET
    staging_bucket = STAGING_BUCKET
    dss_uploader = DssUploader(dss_endpoint, staging_bucket)
    metadata_file_uploader = MetadataFileUploader(dss_uploader)
    bundle_uploader = BundleUploader(dss_uploader, metadata_file_uploader)

    # Testing/Troubleshooting Start
    # key = "topmed_open_access/014a9de5-cb88-5e37-a196-b6e3ab30fff6/NWD759405.recab.cram"
    # bundle_uuid = "014a9de5-cb88-5e37-a196-b6e3ab30fff6"
    # file_uuid = str(uuid.uuid4())
    # result = dss_uploader.upload_cloud_file(bucket, key, bundle_uuid, file_uuid)
    # print(result)
    #
    # result = dss_uploader.upload_local_file(LOCAL_FILE, bundle_uuid)
    # print(result)
    #
    # filename = DssUploader.get_filename_from_key(key)
    # result = metadata_file_uploader.load_file(bucket, key, filename, SCHEMA_URL, bundle_uuid)
    # print(result)
    #
    # result = bundle_uploader.load_bundle(bucket, BUNDLE_PATH, BUNDLE_UUID)
    # print(result)
    # Testing/Troubleshooting Start

    bundle_uploader.load_all_bundles(bucket, BUNDLES_PATH)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.INFO)
    suppress_verbose_logging()
    main(sys.argv[1:])
