#!/usr/bin/env python

"""
Utility to load data files and bundles located in AWS and/or GCP into the HCA Data Storage System (DSS).

The Google controlled access buckets are based on ACLs for user accounts
Before running this loader, configure use of Google user account, run: gcloud auth login
"""
import binascii
import csv
import json
import logging
import sys
import time
import typing
import uuid
from urllib.parse import urlparse

import base64
import boto3
import botocore
import os
import requests
from boto3.s3.transfer import TransferConfig
from cloud_blobstore import BlobStore, s3
from collections import defaultdict
from google.cloud.storage import Client
from hca.dss import DSSClient
from hca.util import SwaggerAPIException
from io import open

from packages.checksumming_io.checksumming_io import ChecksummingSink, S3Etag
from upload_to_cloud import upload_to_cloud

logger = logging.getLogger(__name__)

SCHEMA_URL = ("https://raw.githubusercontent.com/DataBiosphere/commons-sample-data/master"
              "/json_schema/spinnaker_metadata/1.2.1/spinnaker_metadata_schema.json")
SCHEMA_VERSION = "1.2.1"
SCHEMA_TYPE = "spinnaker_metadata"

DSS_ENDPOINT_DEFAULT = "https://commons-dss.ucsc-cgp-dev.org/v1"

# Google Cloud Access
GOOGLE_PROJECT_ID = "platform-dev-178517"  # For requester pays

# Default values for "topmed_open_access" data set
SOURCE_BUCKET_DEFAULT = "cgp-commons-public"
STAGING_BUCKET_DEFAULT = "commons-dss-staging"
SOURCE_BUNDLE_PREFIX_DEFAULT = "topmed_open_access"

# Default values for "topmed_12k" data set
# TODO Change to get from original/master location in Google?
MANIFEST_BUCKET_DEFAULT = "mbaumann-general"
MANIFEST_KEY_DEFAULT = "commonsTOPMed12k/manifest.data-commons-pilot.txt"
METADATA_BUCKET_DEFAULT = "topmed12k-redwood-storage"
METADATA_PREFIX_DEFAULT = "data"
METADATA_CACHE_BUCKET_DEFAULT = "mbaumann-general"
METADATA_CACHE_KEY_DEFAULT = "commonsTOPMed12k/metadata_cache.json"

CREATOR_ID = 20


class DssUploader:
    def __init__(self, dss_endpoint: str, staging_bucket: str, dry_run: bool) -> None:
        self.dss_endpoint = dss_endpoint
        self.staging_bucket = staging_bucket
        self.dry_run = dry_run
        self.s3_client = boto3.client("s3")
        self.blobstore = s3.S3BlobStore(self.s3_client)
        self.gs_client = Client()
        os.environ.pop('HCA_CONFIG_FILE', None)
        self.dss_client = DSSClient()
        self.dss_client.host = "https://commons-dss.ucsc-cgp-dev.org/v1"

    def upload_cloud_file(self, bucket, key, bundle_uuid, file_uuid) -> tuple:
        if not self._has_hca_tags(self.blobstore, bucket, key):
            checksums = self._calculate_checksums(self.s3_client, bucket, key)
            self._set_hca_metadata_tags(self.s3_client, bucket, key, checksums)
        return self._upload_tagged_cloud_file_to_dss(bucket, key, file_uuid, bundle_uuid)

    def upload_cloud_file_by_reference(self, filename: str, file_cloud_urls: set, bundle_uuid: str , file_uuid: str) -> tuple:
        if self.dry_run:
            logger.info(f"DRY RUN: upload_cloud_file_by_reference: {filename} {str(file_cloud_urls)} {bundle_uuid}")
        file_reference = self._create_file_reference(file_cloud_urls)
        return self.upload_dict_as_file(file_reference, filename, bundle_uuid, "application/json; dss-type=fileref")

    def _create_file_reference(self, file_cloud_urls: set) -> dict:
        s3_metadata = None
        gs_metadata = None
        for cloud_url in file_cloud_urls:
            url = urlparse(cloud_url)
            bucket = url.netloc
            key = url.path[1:]
            if url.scheme == "s3":
                s3_metadata = self._get_s3_file_metadata(bucket, key)
            elif url.scheme == "gs":
                gs_metadata = self._get_gs_file_metadata(bucket, key)
            else:
                logger.warning("Unsupported cloud URL scheme: {cloud_url}")
        return self._consolidate_metadata(file_cloud_urls, s3_metadata, gs_metadata)

    def _get_s3_file_metadata(self, bucket: str, key: str) -> dict:
        metadata = dict()
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            metadata['content-type'] = response['ContentType']
            metadata['s3_etag'] = response['ETag']
            metadata['size'] = response['ContentLength']
        except botocore.exceptions.ClientError as e:
            logger.warning(f"Error accessing s3://{bucket}/{key} Exception: {e}")
        return metadata

    def _get_gs_file_metadata(self, bucket: str, key: str) -> dict:
        metadata = dict()
        try:
            gs_bucket = self.gs_client.bucket(bucket, GOOGLE_PROJECT_ID)
            blob_obj = gs_bucket.get_blob(key)
            metadata['content-type'] = blob_obj.content_type
            metadata['crc32c'] = binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower()
            metadata['size'] = blob_obj.size
        except Exception as e:
            logger.warning(f"Error accessing s3://{bucket}/{key} Exception: {e}")
        return metadata

    @staticmethod
    def _consolidate_metadata(file_cloud_urls: set, s3_metadata: dict, gs_metadata: dict) -> dict:
        consolidated_metadata = s3_metadata.copy()
        consolidated_metadata.update(gs_metadata)
        consolidated_metadata['url'] = list(file_cloud_urls)
        return consolidated_metadata

    def upload_dict_as_file(self, value: dict, filename: str, bundle_uuid: str, content_type=None):
        file_path = "/tmp/" + filename
        with open(file_path, "w") as fh:
            fh.write(json.dumps(value, indent=4))
        result = self.upload_local_file(file_path, bundle_uuid, content_type)
        os.remove(file_path)
        return result

    def upload_local_file(self, path, bundle_uuid: str, content_type=None):
        file_uuid, key = self._upload_local_file_to_staging(path, content_type)
        return self._upload_tagged_cloud_file_to_dss(self.staging_bucket, key, file_uuid, bundle_uuid)

    def load_bundle(self, file_info_list: list, bundle_uuid: str):
        kwargs = dict(replica="aws", creator_uid=CREATOR_ID, files=file_info_list, uuid=bundle_uuid)
        if not self.dry_run:
            response = self.dss_client.put_bundle(**kwargs)
            version = response['version']
        else:
            logger.info("DRY RUN: DSS put bundle: " + str(kwargs))
            version = None
        bundle_fqid = f"{bundle_uuid}.{version}"
        logger.info(f"Loaded bundle: {bundle_fqid}")
        return bundle_fqid

    @staticmethod
    def get_filename_from_key(key: str):
        return key.split("/")[-1]

    def _upload_local_file_to_staging(self, path: str, content_type):
        with open(path, "rb") as fh:
            file_uuids, key_names = upload_to_cloud([fh], self.staging_bucket, "aws", False, content_type)
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
        filename = self.get_filename_from_key(source_key)

        if self.dry_run:
            logger.info(f"DRY RUN: _upload_tagged_cloud_file_to_dss: {source_bucket} {source_key} {file_uuid} {bundle_uuid}")
            return file_uuid, None, filename

        request_parameters = dict(uuid=file_uuid, bundle_uuid=bundle_uuid, creator_uid=CREATOR_ID, source_url=source_url)
        if self.dry_run:
            print("DRY RUN: put file: " + str(request_parameters))
            return file_uuid, None, filename
        response = self.dss_client.put_file._request(request_parameters)
        file_version = response.json().get('version', "blank")

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

        return file_uuid, file_version, filename


class MetadataFileUploader:
    def __init__(self, dss_uploader: DssUploader) -> None:
        self.dss_uploader = dss_uploader

    def load_file(self, bucket: str, key: str, filename: str, schema_url: str, schema_version: str, schema_type: str,
                  bundle_uuid: str) -> tuple:
        metadata_string = self.dss_uploader.blobstore.get(bucket, key).decode("utf-8")
        metadata = json.loads(metadata_string)
        metadata['core'] = dict(schema_url=schema_url, schema_version=schema_version, type="metadata")
        return self.dss_uploader.upload_dict_as_file(metadata, "metadata.json", bundle_uuid)


class BundleUploaderForTopMedOpenAccess:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    def load_bundle(self, bucket, bundle_key, bundle_uuid):
        file_info_list = self._load_bundle_files(bucket, bundle_key, bundle_uuid)
        return self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def load_all_bundles(self, bucket, bundles_key, start_after_key):
        count = 0
        for key in self.dss_uploader.blobstore.list_v2(bucket, bundles_key, start_after_key):
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

class BundleUploaderForTopMed12k:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader,
                 manifest_bucket: str, manifest_key: str,
                 manifest_start_index: int, manifest_end_index: int,
                 metadata_bucket: str, metadata_prefix: str,
                 metadata_cache_bucket: str, metadata_cache_key: str) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader
        self.manifest_bucket = manifest_bucket
        self.manifest_key = manifest_key
        self.manifest_start_index = manifest_start_index
        self.manifest_end_index = manifest_end_index
        self.metadata_bucket = metadata_bucket
        self.metadata_prefix = metadata_prefix
        self.metadata_cache_bucket = metadata_cache_bucket
        self.metadata_cache_key = metadata_cache_key

    def load_bundle(self, metadata_bucket: str, metadata_key: str, data_files: dict, bundle_uuid: str):
        file_info_list = self._load_bundle_files(metadata_bucket, metadata_key, data_files, bundle_uuid)
        return self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def load_all_bundles(self):
        map_specimen_id_to_cloud_files = self._get_map_specimen_id_to_cloud_files()
        map_specimen_id_to_metadata = self._get_map_specimen_id_to_metadata()
        bundles_loaded_count = 0
        for specimen_id in map_specimen_id_to_cloud_files.keys():
            if map_specimen_id_to_metadata.get(specimen_id):
                bundles_loaded_count += 1
                metadata_file_key = map_specimen_id_to_metadata[specimen_id]['key']
                bundle_uuid = map_specimen_id_to_metadata[specimen_id]['json']['specimen'][0]['samples'][0]['analysis'][0]['bundle_uuid']
                self.load_bundle(self.metadata_bucket, metadata_file_key,
                                 map_specimen_id_to_cloud_files[specimen_id],
                                 bundle_uuid)
        print(f"match count: {bundles_loaded_count}")

    def _load_bundle_files(self, metadata_bucket: str, metadata_key: str, data_files: dict, bundle_uuid: str) \
            -> typing.List[typing.Dict[str, str]]:
        file_info_list = []
        # Load metadata file
        file_uuid, file_version, filename = \
            self.metadata_file_uploader.load_file(metadata_bucket, metadata_key, "metadata.json",
                                                  SCHEMA_URL, SCHEMA_VERSION, SCHEMA_TYPE,
                                                  bundle_uuid)
        file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=True))

        # Load data files by reference
        for filename, file_cloud_urls in data_files.items():
            file_uuid = str(uuid.uuid4())
            file_uuid, file_version, filename = self.dss_uploader.upload_cloud_file_by_reference(
                filename, file_cloud_urls, bundle_uuid, file_uuid)
            file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))
        return file_info_list

    def _get_map_specimen_id_to_cloud_files(self):
        manifest_text = self.dss_uploader.blobstore.get(self.manifest_bucket, self.manifest_key).decode("utf-8")
        reader = csv.reader(manifest_text.split('\n'), delimiter='\t')
        raw_data = [columns for columns in reader]
        map_specimen_to_files = defaultdict(dict)
        for index, row in enumerate(raw_data[1:]):
            if index < self.manifest_start_index:
                continue
            if index >= self.manifest_end_index:
                break
            if len(row) == 0:
                break
            logger.info(f"Processing manifest row: {index}: {row}")
            specimen = row[3] # sra_sample_id
            for file_index in range(25, 29):
                file_url = row[file_index]
                filename = file_url.split("/")[-1]
                if map_specimen_to_files[specimen].get(filename):
                    map_specimen_to_files[specimen][filename].add(file_url)
                else:
                    map_specimen_to_files[specimen][filename] = set([file_url])
        return map_specimen_to_files

    def _get_map_specimen_id_to_metadata(self):
        cached_metadata = self._get_cached_metadata()
        if cached_metadata is not None:
            return cached_metadata
        map_specimen_id_to_metadata = dict()
        metadata_candidate_keys = self.dss_uploader.blobstore.list(self.metadata_bucket, self.metadata_prefix)
        count = 0
        for metadata_key in metadata_candidate_keys:
            count += 1
            if not metadata_key.endswith(".meta"):
                try:
                    metadata_string = None
                    if self.dss_uploader.blobstore.get_size(self.metadata_bucket, metadata_key) > 12:
                        logger.info("Reading metadata file %i: %s", count, "/".join([self.metadata_bucket, metadata_key]))
                        metadata_string = self.dss_uploader.blobstore.get(self.metadata_bucket, metadata_key).decode("utf=8")
                        metadata_json = json.loads(metadata_string)
                        specimen_id = metadata_json['specimen'][0]['submitter_specimen_id']
                        map_specimen_id_to_metadata[specimen_id] = dict(key=metadata_key, json=metadata_json)
                except Exception as e:
                    logger.error(f"Exception occurred processing metadata file: {metadata_key}, {e}, {metadata_string}", exc_info=True)
        self._write_metadata_cache(map_specimen_id_to_metadata)
        return map_specimen_id_to_metadata

    def _get_cached_metadata(self):
        s3 = boto3.resource('s3')
        try:
            s3.Object(self.metadata_cache_bucket, self.metadata_cache_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info(f"No previously cached metadata found in: s3://{self.metadata_cache_bucket}/{self.metadata_cache_key}")
                return None
            else:
                raise
        logger.info(f"Loading previously cached metadata from: s3://{self.metadata_cache_bucket}/{self.metadata_cache_key}")
        metadata_cache_string = self.dss_uploader.blobstore.get(self.metadata_cache_bucket, self.metadata_cache_key).decode("utf=8")
        metadata_cache_json = json.loads(metadata_cache_string)
        return metadata_cache_json

    def _write_metadata_cache(self, metadata: dict):
        s3 = boto3.resource('s3')
        object = s3.Object(self.metadata_cache_bucket, self.metadata_cache_key)
        metadata_string = json.dumps(metadata)
        object.put(Body=metadata_string.encode())

    def clear_metadata_cache(self, dry_run: bool):
        if not dry_run:
            boto3.client("s3").delete_object(Bucket=self.metadata_cache_bucket, Key=self.metadata_cache_key)


def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Output actions that would otherwise be performed.")
    group.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                       help="Perform the actions.")
    parser.add_argument("--dss-endpoint", metavar="DSS_ENDPOINT", required=False,
                        default=DSS_ENDPOINT_DEFAULT,
                        help="The HCA Data Storage System endpoint to use.")
    parser.add_argument("--source-bucket", metavar="SOURCE_BUCKET", required=False,
                        default=SOURCE_BUCKET_DEFAULT,
                        help="The bucket containing the bundles to load.")
    parser.add_argument("--staging-bucket", metavar="STAGING_BUCKET", required=False,
                        default=STAGING_BUCKET_DEFAULT,
                        help="The bucket to stage local files for uploading to DSS.")

    subparsers = parser.add_subparsers(dest='data_set', help='Data set to load')
    parser_topmed_open_access = subparsers.add_parser("topmed_open_access", help='Load "topmed_open_access"')
    parser_topmed_open_access.add_argument("--source-bundle-prefix", metavar="SOURCE_BUNDLE_PREFIX", required=False,
                                           default=SOURCE_BUNDLE_PREFIX_DEFAULT,
                                           help="The path prefix to the bundle(s) to load.")
    parser_topmed_open_access.add_argument("--start-after-key", metavar="START_AFTER_KEY", required=False,
                                           help="The key after which to begin processing.")

    parser_topmed_12k = subparsers.add_parser("topmed_12k", help='Load "topmed_12k"')
    parser_topmed_12k.add_argument("--manifest-bucket", metavar="MANIFEST_BUCKET", required=False,
                                   default=MANIFEST_BUCKET_DEFAULT,
                                   help="The bucket containing the manifest that identifies files to load.")
    parser_topmed_12k.add_argument("--manifest-key", metavar="MANIFEST_KEY", required=False,
                                   default=MANIFEST_KEY_DEFAULT,
                                   help="The key for the manifest that identifies files to load.")
    parser_topmed_12k.add_argument("--manifest-start-index", metavar="MANIFEST_START_INDEX", type=int, required=False,
                                   default=0,
                                   help="The manifest inclusive starting index")
    parser_topmed_12k.add_argument("--manifest-end-index", metavar="MANIFEST_END_INDEX", type=int, required=False,
                                   default=1000000,
                                   help="The manifest exclusive end index")
    parser_topmed_12k.add_argument("--metadata-bucket", metavar="METADATA_BUCKET", required=False,
                                   default=METADATA_BUCKET_DEFAULT,
                                   help="The bucket containing the metadata files.")
    parser_topmed_12k.add_argument("--metadata-prefix", metavar="METADATA_PREFIX", required=False,
                                   default=METADATA_PREFIX_DEFAULT,
                                   help="The prefix to the location of the metadata files.")
    parser_topmed_12k.add_argument("--metadata-cache-bucket", metavar="METADATA_CACHE_BUCKET", required=False,
                                   default=METADATA_CACHE_BUCKET_DEFAULT,
                                   help="The bucket containing the metadata cache file.")
    parser_topmed_12k.add_argument("--metadata-cache-key", metavar="METADATA_CACHE_KEY", required=False,
                                   default=METADATA_CACHE_KEY_DEFAULT,
                                   help="The key of the metadata cache file.")
    parser_topmed_12k.add_argument("--clear-metadata-cache", required=False,
                                   default=False, action="store_true",
                                   help="Clear the metadata cache.")
    options = parser.parse_args(argv)

    # Clear configured credentials, which are likely for service accouts.
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    os.environ.pop('GOOGLE_APPLICATION_SECRETS', None)

    dss_uploader = DssUploader(options.dss_endpoint, options.staging_bucket, options.dry_run)
    metadata_file_uploader = MetadataFileUploader(dss_uploader)

    if options.data_set == "topmed_open_access":
        bundle_uploader = BundleUploaderForTopMedOpenAccess(dss_uploader, metadata_file_uploader)
        bundle_uploader.load_all_bundles(options.source_bucket,
                                         options.source_bundle_prefix,
                                         options.start_after_key)
    elif options.data_set == "topmed_12k":
        bundle_uploader = BundleUploaderForTopMed12k(dss_uploader, metadata_file_uploader,
                                                     options.manifest_bucket, options.manifest_key,
                                                     options.manifest_start_index, options.manifest_end_index,
                                                     options.metadata_bucket, options.metadata_prefix,
                                                     options.metadata_cache_bucket, options.metadata_cache_key)
        if options.clear_metadata_cache:
            bundle_uploader.clear_metadata_cache(options.dry_run)
        bundle_uploader.load_all_bundles()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.INFO)
    suppress_verbose_logging()
    main(sys.argv[1:])
