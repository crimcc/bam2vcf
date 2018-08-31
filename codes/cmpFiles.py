"""
# Purpose     : To check if outputs are well produced
# Descriptions:
#  - This codes compares two files and find same and differences
#
# Start date  : Jun 28, 2018
# Last update : Jun 28, 2018
"""

__author__ 		= "Jong Cheol Jeong"
__copyright__ 	= "Copyright 2018, UK Cancer Research Informatics"
__version__ 	= "1.0.0"
__maintainer__ 	= "Jong Cheol Jeong"
__email__ 		= "jjeong@kcr.uky.edu"

import pandas as pd
import os
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
parser.add_argument("-r", "--ref", help='list of file names used for reference and compared to a target file', action='store', required=True)
parser.add_argument("-t", "--target", help='list of file names and compared to a target file', action='store', required=True)
parser.add_argument("-o", "--out", help='output path to store the results', action='store', required=True)
parser.add_argument("-i", "--refidx", help='the column index of reference file to be used DEFAULT=0', action='store', type=int, default=0)
parser.add_argument("-j", "--tgidx", help='the column index of target file to be used DEFAULT=0', action='store', type=int, default=0)


args = parser.parse_args()

refFile = args.ref
tgFile  = args.target
outPath = args.out
ridx    = args.refidx
tidx    = args.tgidx

try:
    os.makedirs(outPath)
except OSError:
    pass


# Read files and search matched ones
#------------------------------------------------------------------------------

dfRef = pd.read_table(refFile, sep='\t', header=None)
dfTg  = pd.read_table(tgFile, sep='\t', header=None)

outMatch = '{}/matched.txt'.format(outPath)
outNone  = '{}/missing.txt'.format(outPath)

with open(outMatch, 'w') as fm:
    with open(outNone, 'w') as fn:
        for refname in dfRef.loc[:,ridx]:
            refnameShort=refname.split('/')[-1].split('.')[0]
            tmp = dfTg.index[dfTg.iloc[:,tidx].str.contains(refnameShort) == True].tolist()
            if len(tmp) > 0:
                #m = 'MATCH >> Ref={}\tTg={}'.format(refname, dfTg.iloc[tmp[0],0])
                fname = '{}\n'.format(refname.strip())
                fm.write(fname)

            else:
                #m = 'NONE >> Ref={} cannot found'.format(refname)
                fname = '{}\n'.format(refname.strip())
                fn.write(fname)
