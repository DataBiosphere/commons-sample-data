import boto3
import ssl
import urllib2
import json
import io
import botocore
from tqdm import tqdm

ENDPOINT = 'commons.ucsc-cgp-dev.org'
REDWOOD_BUCKET = 'commons-redwood-storage'
COMMONS_BUCKET = 'cgp-commons-public'


class MetadataServerAPI:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def get_metadata_by_file_name(self, file_name):
        url = 'https://metadata.{}/entities?fileName={}'.format(
            self.endpoint, file_name)
        return self._make_api_call(url)

    def get_metadata_json_file_metadata(self, bundle_id):
        url = 'https://metadata.{}/entities?fileName={}&gnosId={}'.format(
            self.endpoint, 'metadata.json', bundle_id)
        return self._make_api_call(url)

    def _make_api_call(self, url):
        # print url
        context = self._generate_fake_context()
        return json.load(urllib2.urlopen(url, context=context))

    @staticmethod
    def _generate_fake_context():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx


def show_progress(progress_bar):
    def inner(bytes_delta):
        progress_bar.update(bytes_delta)
    return inner


s3_client = boto3.client('s3')

res = s3_client.list_objects_v2(Bucket=COMMONS_BUCKET)
wrp = MetadataServerAPI(ENDPOINT)


msng_cnt = 0
for c in res['Contents']:
    paths = c['Key'].split('/')
    # print 'checking', paths
    if len(paths) < 3:
        file_name = paths[-1]
        found_files = wrp.get_metadata_by_file_name(file_name)['content']

        if len(found_files) == 1:
            bundle_id = found_files[0]['gnosId']
            try:
                mj_id = wrp.get_metadata_json_file_metadata(bundle_id)['content'][0]['id']
            except KeyError:
                print 'no metadata.json', file_name
            else:
                metadata_json = io.BytesIO()
                try:
                    s3_client.download_fileobj(REDWOOD_BUCKET,
                                               "{}/{}".format('data', mj_id),
                                               metadata_json)
                except botocore.exceptions.ClientError:
                    print 'not in bucket', "{}/{}".format('data', mj_id)
                metadata_json.seek(0)
                new_folder = 'topmed_open_access/{}'.format(bundle_id)
                res = s3_client.upload_fileobj(metadata_json, COMMONS_BUCKET,
                                               '{}/metadata.json'.format(
                                                   new_folder))

                s3_res = boto3.resource('s3')
                copy_source = {
                    'Bucket': COMMONS_BUCKET,
                    'Key': c['Key']
                }
                filesize = s3_client.head_object(**copy_source)['ContentLength']
                with tqdm(total=filesize, unit='B', unit_scale=True,
                          desc=c['Key']) as bar:
                    s3_res.meta.client.copy(copy_source, COMMONS_BUCKET,
                                            "{}/{}".format(new_folder, file_name),
                                            Callback=show_progress(bar))
                s3_client.delete_object(**copy_source)
        elif len(found_files) == 0:
            print 'missing', file_name
            msng_cnt += 1
        else:
            print 'dupes found', file_name
            print found_files
    else:
        print 'Already organized', c['Key']
