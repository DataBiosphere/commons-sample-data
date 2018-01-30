A dataset in a requestor pays bucket by Goncalo's group.  You need to be on an authorized user list to access.

```
gsutil -u platform-dev-178517 cp gs://topmed-irc-share/genomes/readme.data-commons-pilot.txt .
gsutil -u platform-dev-178517 cp gs://topmed-irc-share/genomes/manifest.data-commons-pilot.txt .
```

Please talk to Doug or Moran to be added to the GROUP_TOPMed-Authorized@firecloud.org to get access on google cloud.  Right now this is only setup for Data Biosphere developers, a future process is being worked on for the public.

The info from the readme:

```
Column labels in the current draft TOPMed sample metadata file:
   1    NWD_ID                  =  TOPMed sequencing instance identifier
   2    biosample_id            =  dbGaP "SAMN" sample identifier
   3    dbgap_sample_id         =  dbGaP numeric sample identifier
   4    sra_sample_id           =  SRA "SRS" DNA sample identifier
   5    submitted_subject_id    =  originating study's deidentified subject identifier
   6    dbgap_subject_id        =  dbGaP numeric subject identifier
   7    consent_short_name      =  dbGaP consent code
   8    sex                     =  subject sex (if provided by the originating study)
   9    body_site               =  dbGaP sample descriptor
  10    analyte_type            =  dbGaP sample descriptor
  11    sample_use              =  dbGaP sample use description
  12    dbGaP_access_date       =  download date for dbGaP telemetry information
  13    phs                     =  TOPMed sequencing phs number (if registered in dbGaP)
  14    dbGaP_study_name        =  TOPMed sequencing study name (if registered in dbGaP)
  15    TOPMed_abbrev           =  TOPMed project abbreviation
  16    TOPMed_project          =  TOPMed project
  17    PI_name                 =  TOPMed project PI
  18    seq_center              =  sequencing center
  19    receipt_group           =  batch number for receipt of sequence data in Michigan
  20    datemapping_b38         =  date of Michigan re-mapping to build 38
  21    unique_reads            =  number of unique sequence reads
  22    read_length             =  sequencing read length
  23    file_size_Gb            =  approximate size of build 38 .cram file
  24    md5.cram                =  md5 checksum for build 38 .cram file
  25    md5.crai                =  md5 checksum for .crai index file
  26    s3.cram                 =  path name for build 38 .cram file on Amazon
  27    s3.crai                 =  path name for build 38 .crai file on Amazon
  28    gs.cram                 =  path name for build 38 .cram file on Google
  29    gs.crai                 =  path name for build 38 .crai file on Google

```
