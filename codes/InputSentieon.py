"""
# Purpose     : Write Sentieon Inputs and Output pairs
# Descriptions:
#  - This codes reads the list of inputs and write input output pairs
#
# Start date  : July 6, 2018
# Last update : July 6, 2018
"""

__author__ 		= "Jong Cheol Jeong"
__copyright__ 	= "Copyright 2018, UK Cancer Research Informatics"
__version__ 	= "1.0.0"
__maintainer__ 	= "Jong Cheol Jeong"
__email__ 		= "jjeong@kcr.uky.edu"

import argparse

"""
#------------------------------------------------------------------------------
# Define parameters
# - refFile: reference file to be compared - automatically generated if running by dsub.py
#            e.g.,) For BAM file indexing
#                   xxxx.bam is refFiles
#                   xxxx.bam.bai is TgFiles
#                   Therefore, it searches 'xxx.bam' in Target name 'xxx.bam.bai'
#
# - tgFile : list of output files can be produced by following:
#       gsutil ls gs://my-bam |grep 'bam$' > bamList.txt
#           OR
#       gsutil ls gs://my-bam |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help='a file containing the list of bam files in google storage', action='store', required=True)
parser.add_argument("-o", "--output", help='name of output file if path is not specified then the file will be stored in the same directory of input file', action='store', required=True)
parser.add_argument("-s", "--gs", help='Google Cloud storage to store the results', action='store', required=True)

args = parser.parse_args()

inFile = args.input
outFile = args.output
outBam = args.gs

# check OutFile Path
#------------------------------------------------------------------------------
pathList = outFile.strip().split('/')
if len(pathList) < 2:
    outFile = "{}/{}".format('/'.join(inFile.strip().split('/')[:-1]), outFile)


# Read files and search matched ones
#------------------------------------------------------------------------------
inList  = ['BAM']
outList = ['OUTPUT_BUCKET']

with open(inFile, 'r') as f:
    for line in f:
        inList.append(line.strip())
        pathList = line.strip().split('/')
        nameList = pathList[len(pathList)-1].split('.')
        dirName = nameList[0]
        newName = "{}/{}".format(outBam, dirName)
        outList.append(newName)


assert(len(inList) == len(outList)), "The number of inputs and outputs are different\nPlease check the {}\n".format(inFile)

# Write data
#------------------------------------------------------------------------------
with open(outFile, 'w') as f:
    for i in range(len(outList)):
        cmt = "{}\t{}\n".format(inList[i], outList[i])
        f.writelines(cmt)
