"""
# Purpose     : To submit Google Cloud GATK pipelines
# Descriptions:
#  - Codes contain functions to submit jobs through GCP Genomic Pipelines
#  - current GATK 4.0 pipeline only supports Human genome reference GRCh38/hg38
#
# Start date  : July 23, 2018
# Last update : Aug 21, 2018
"""

__author__ 		= "Jong Cheol Jeong"
__copyright__ 	= "Copyright 2018, UK Cancer Research Informatics"
__version__ 	= "1.0.0"
__maintainer__ 	= "Jong Cheol Jeong"
__email__ 		= "jjeong@kcr.uky.edu"

import dsub
import os
import argparse


"""
#------------------------------------------------------------------------------
# Defining parameters
#------------------------------------------------------------------------------
"""

"""
#------------------------------------------------------------------------------
# Define parameters
# list of BAM can be obtained with gsutil (https://cloud.google.com/storage/docs/gsutil_install) 
# gsutil ls gs://bam-vcf-unmapbam |grep 'bam$' > listUnmappedBam.txt
# OR
# gsutil ls gs://bam-vcf-unmapbam |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > listUnmappedBam.txt
#
# The process ID will be stored in the directory given by '--script' option
# To check a job status, please run folloing command
# $ bash /my/script/directory/FileName.txt
#
"""

"""
# < Example running command >
python runGenPipe.py -i /sentieon/dsub/inputs/short_listUnmappedBam.txt \
-o gs://jc-gatk-out \
-s /sentieon/dsub/inputs/Scripts/test \
-g /sentieon/gcgp/broad-prod-wgs-germline-snps-indels \
-w /sentieon/gcgp/wdl 
#------------------------------------------------------------------------------
"""

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help='list of unmapped BAM files in cloud storage to be run', action='store', required=True)
parser.add_argument("-o", "--output", help='output google storage directory i.e., GS Path (gs://your-bucket)', action='store', required=True)
parser.add_argument("-s", "--script", help='local directory to store working scripts', action='store', required=True)
parser.add_argument("-g", "--gatkdir", help='The path of GATK Best Practices Pipeline templates e.g., /usr/local/broad-prod-wgs-germline-snps-indels\nThis can be downloaded from https://cloud.google.com/genomics/docs/tutorials/gatk', action='store', required=True)
parser.add_argument("-z", "--zone", help='List of Google Compute Engine availability zones to which resource creation will restricted. [Default="us-central1-f"]', type=str, default='us-central1-f')
parser.add_argument("-w", "--wdl", help='WDL directory found in GATK Best Practices Pipeline examples. e.g., /usr/local/wdl\nThis can be downloaded from https://cloud.google.com/genomics/docs/tutorials/gatk', action='store', required=True)
parser.add_argument("-x", "--prefix", help='Prefix template e.g., "PairedEndSingleSampleWf" /usr/local/wdl\nThis can be downloaded from https://cloud.google.com/genomics/docs/tutorials/gatk [Default = "PairedEndSingleSampleWf"] ', type=str, default='PairedEndSingleSampleWf')

args = parser.parse_args()

listBAM         = args.input
GATK_OUT_DIR    = args.output
scPath          = args.script
GATK_GOOGLE_DIR = args.gatkdir
WDL_DIR         = args.wdl
plPrefix        = args.prefix
Zones           = args.zone

try:
    os.makedirs(scPath)
except OSError:
    pass

# Read input files and preparing for output name by adding '.head'
# in front of the file extention 'bam'
#------------------------------------------------------------------------------
inBAM = []
outGS = []
tgPath = listBAM.split('/')
tgPath = '/'.join(tgPath[0:len(tgPath)-1])
with open(listBAM, 'r') as f:
    for line in f:
        inBAM.append(line.strip())
        pathList = line.strip().split('/')
        preFix = pathList[-1].split('.')[0]
        newName = "{}/{}".format(GATK_OUT_DIR, preFix)
        outGS.append(newName)


#-- check if the number of inputs and outputs are same
assert(len(inBAM) == len(outGS)), "The number of inputs and outputs are different\nPlease check the {}\n".format(listBAM)

#-- Write input & output mapping file and store them into the same location with listBAM
mapBAM = listBAM.split('/')
mapBAM = "{}/mapBAM-GS.txt".format('/'.join(mapBAM[:len(mapBAM)-1]))
with open(mapBAM, 'w') as f:
    for i in range(len(inBAM)):
        cmt = "{}\t{}\n".format(inBAM[i], outGS[i])
        f.writelines(cmt)


"""
#------------------------------------------------------------------------------
# Submit jobs
#------------------------------------------------------------------------------
"""


for i in range(len(inBAM)):
    ibam = inBAM[i]
    obam = outGS[i]
    cmt = "[{}/{}] {} is processing...".format(i + 1, len(inBAM), ibam)
    print(cmt)
    LogGS = '{}/logs'.format(obam)
    dsub.subGenPipe(Zones=Zones, Logs=LogGS, inFile=ibam, scriptPath=scPath, GATK_GOOGLE_DIR=GATK_GOOGLE_DIR, GATK_OUT_DIR=obam, WDL_DIR=WDL_DIR, plPrefix=plPrefix)
    print('\n')
