"""
# Purpose     : create BAM index
# Descriptions:
#  - Codes contain functions to submit jobs via Google Cloud 'dsub'
#  - This codes must be run after running 'addPL.py' that correct known issue of PL absence
# Start date  : July 2, 2018
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
# Define parameters
# list of BAM can be obtained with gsutil (https://cloud.google.com/storage/docs/gsutil_install) 
# gsutil ls gs://my-bam3 |grep 'bam$' > bam_idx.txt
# OR
# gsutil ls gs://my-bam3 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bam_idx.txt
#------------------------------------------------------------------------------
"""
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help='list of file names in cloud storage to be run', action='store', required=True)
parser.add_argument("-o", "--output", help='output google storage directory i.e., GS Path (gs://your-bucket', action='store', required=True)
parser.add_argument("-s", "--script", help='local directory to store dsub scripts', action='store', required=True)
parser.add_argument("-p", "--project", help='Google project ID (e.g., my-project-name)', action='store', required=True)

args = parser.parse_args()

listBAM = args.input
tgPath  = args.output
logPath = "{}/log".format(tgPath)
scPath = args.script
prjName = arg.project

try:
    os.makedirs(scPath)
except OSError:
    pass


# Read input files and preparing for output name by adding '.head'
# in front of the file extention 'bam'
#------------------------------------------------------------------------------
inBAM = []
outBAM = []
Scripts = []

with open(listBAM, 'r') as f:
    for line in f:
        inBAM.append(line.strip())
        pathList = line.strip().split('/')
        nameList = pathList[len(pathList)-1].split('.')
        newName = "{}/{}.{}.bai".format(tgPath, '.'.join(nameList[:(len(nameList)-1)]), nameList[len(nameList)-1])
        outBAM.append(newName)

#-- check if the number of inputs and outputs are same
assert(len(inBAM) == len(outBAM)), "The number of inputs and outputs are different\nPlease check the {}\n".format(listBAM)

#-- Write input & output mapping file and store them into the same location with listBAM
mapBAM = listBAM.split('/')
mapBAM = "{}/mapBAM-index.txt".format('/'.join(mapBAM[:len(mapBAM)-1]))
with open(mapBAM, 'w') as f:
    for i in range(len(inBAM)):
        cmt = "{}\t{}\n".format(inBAM[i], outBAM[i])
        f.writelines(cmt)


"""
#------------------------------------------------------------------------------
# Submit jobs
#------------------------------------------------------------------------------
"""
for i in range(len(inBAM)):
    ibam = inBAM[i]
    obam = outBAM[i]
    oScr = "{}/dsub_{}.sh".format(scPath, str(i).zfill(3))
    cmt = "[{}/{}] {} is processing...".format(i + 1, len(inBAM), ibam)
    print(cmt)
    dsub.BuildBamIndex(prjName=prjName, inFile=ibam, outFile=obam, scriptPath=oScr, Logs=logPath)
    print('\n')
