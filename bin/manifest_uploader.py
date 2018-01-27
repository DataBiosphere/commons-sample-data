import BeautifulSoup
import urllib2
import csv
import uuid
import random
import subprocess
import os
import shutil

# TODO move vars to env file
ACCESS_TOKEN = "{Add token here}"
SPINNAKER_DOCKER_IMAGE = "test-core:dev"
SPINNAKER_DIRECTORY = '/home/ubuntu/dcc/dcc-spinnaker-client'
SPINNAKER_OUTPUTS_DIR = 'outputs'

DOWNLOAD_DIRECTORY = "samples"
DEV_ENDPOINT = 'commons.ucsc-cgp-dev.org'
TEMP_MANIFEST_NAME = 'topmedifest.tsv'
RAW_TABLE_FILENAME = 'TOPMed.aws.public_samples.manifest.2017.11.30.txt'

rdm = random.Random()


def get_alignment_type(spec_id, element_id):
    source_url = 'https://www.coriell.org/0/Sections/Search/Sample_Detail.aspx'
    query = "Ref={}".format(spec_id)
    dna_page = urllib2.urlopen("{}?{}".format(source_url, query))
    soup = BeautifulSoup.BeautifulSoup(dna_page)
    return soup.find(id=element_id).text


def download_alignment_files(link, save_path):
    subprocess.call("aws s3 cp {} {}".format(link, save_path), shell=True)


def generate_manifest(columns, manifest_filepath):
    with open(manifest_filepath, 'w') as tm_f:
        wrtr = csv.writer(tm_f, delimiter='\t')
        # print SpinnakerManifest().ordered_header_names
        wrtr.writerow(['Program',
                       'Project',
                       'Center Name',
                       'Submitter Donor ID',
                       'Donor UUID',
                       'Submitter Donor Primary Site',
                       'Submitter Specimen ID',
                       'Specimen UUID',
                       'Submitter Specimen Type',
                       'Submitter Experimental Design',
                       'Submitter Sample ID',
                       'Sample UUID',
                       'Analysis Type',
                       'Workflow Name',
                       'Workflow Version',
                       'File Type',
                       'File Path',
                       'Upload File ID',
                       'Data Bundle ID',
                       'Metadata.json'])

        program = 'TOPMed'
        submitter_donor_primary_site = 'B-lymphocyte'
        spec_types = ['Normal - blood derived', 'Normal - other',
                      'Normal - solid tissue', 'Normal - blood']
        rand_num = rdm.randint(0, 3)
        print(rand_num)
        submitter_spec_type = spec_types[rand_num]
        for file_entry in columns[3:]:
            project = 'HapMap' if columns[1][0] == 'N' else '1000 Genomes'
            center_name = columns[2]
            submitter_donor_id = columns[0]
            donor_uuid = ""
            submitter_specimen_id = columns[1]
            specimen_uuid = ""
            submitter_experimental_design = 'WGS'
            submitter_sample_id = submitter_specimen_id + "_sample"
            sample_uuid = ""
            analysis_type = 'sequence_upload'
            workflow_name = 'spinnaker'
            workflow_vers = '1.1.2'
            file_type = file_entry.split('.')[-1]
            file_path = "/samples/" + file_entry.split('/')[-1]
            upload_file_id = ""
            data_bundle_id = ""
            metadata_filepath = ""

            row_data = [program,
                        project,
                        center_name,
                        submitter_donor_id,
                        donor_uuid,
                        submitter_donor_primary_site,
                        submitter_specimen_id,
                        specimen_uuid,
                        submitter_spec_type,
                        submitter_experimental_design,
                        submitter_sample_id,
                        sample_uuid,
                        analysis_type,
                        workflow_name,
                        workflow_vers,
                        file_type,
                        file_path,
                        upload_file_id,
                        data_bundle_id,
                        metadata_filepath]
            wrtr.writerow(row_data)


def upload_file():
    exec_script = "sudo docker run --rm -it" \
                  " -e ACCESS_TOKEN={token} " \
                  " -e REDWOOD_ENDPOINT={endpoint}" \
                  " -v {spin_dir}/{temp_manifest}:/dcc/manifest.tsv" \
                  " -v {spin_dir}/samples:/samples" \
                  " -v {spin_dir}/outputs:/outputs {image} spinnaker-upload" \
                  " --skip-submit" \
                  " /dcc/manifest.tsv".format(token=ACCESS_TOKEN,
                                              endpoint=DEV_ENDPOINT,
                                              temp_manifest=TEMP_MANIFEST_NAME,
                                              image=SPINNAKER_DOCKER_IMAGE,
                                              spin_dir=SPINNAKER_DIRECTORY)
    print exec_script
    subprocess.call(exec_script, shell=True)


def main():
    with open(RAW_TABLE_FILENAME, 'r') as sf:
        rdr = csv.reader(sf, delimiter='\t')
        raw_data = [columns for columns in rdr]

        for x, columns in enumerate(raw_data[1:]):
            manifest_filepath = TEMP_MANIFEST_NAME
            save_folder = DOWNLOAD_DIRECTORY
            print "Generating Manifest..."
            generate_manifest(columns, manifest_filepath)
            alignment_filepaths = []
            has_file = False
            for file_link in columns[3:]:
                save_path = '{}/{}'.format(save_folder, file_link.split('/')[-1])
                alignment_filepaths.append(save_path)
                if os.path.exists(save_path):
                    print "Found {}. Skipped Download".format(save_path)
                else:
                    print "Downloading alignment file ({})...".format(file_link)
                    if os.path.exists(SPINNAKER_OUTPUTS_DIR):
                        shutil.rmtree(SPINNAKER_OUTPUTS_DIR)
                    download_alignment_files(file_link, save_path)
                    has_file = os.path.exists(save_path)
            if not has_file:
                continue
            print "Uploading alignment files..."
            upload_file()
            for filename in alignment_filepaths:
                os.remove(filename)
        print 'Finished Uploading'


if __name__ == "__main__":
    main()
