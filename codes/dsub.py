"""
# Purpose     : To submit multiple jobs via dsub
# Descriptions:
#  - Codes contain functions to submit jobs via Google Cloud 'dsub'
#
# Start date  : May 17, 2018
# Last update : Aug 3, 2018
"""

__author__ 		= "Jong Cheol Jeong"
__copyright__ 	= "Copyright 2018, UK Cancer Research Informatics"
__version__ 	= "1.0.0"
__maintainer__ 	= "Jong Cheol Jeong"
__email__ 		= "jjeong@kcr.uky.edu"


import subprocess
import shutil
import glob
import re
import os
import json
from collections import OrderedDict

"""
#------------------------------------------------------------------------------
# Add PL: variable in BAM file
# :: Example Code ::
# headAddPL(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://jc-gatk-bam |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://jc-gatk-bam |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def headAddPL(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, cmd=None):

    #if prjName is None:
    #    prjName = 'my-project-id'

    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'zlskidmore/samtools:1.4.1'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file to be written. This will be used for submitting jobs (Required!)\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = "samtools view -H ${INFILE} | sed -e 's/SM:\\(.*\\)/SM:\\1\\tPL:illumina/' |samtools reheader -P - ${INFILE} > ${OUTFILE}"


    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process



"""
#------------------------------------------------------------------------------
# Add Cleaning SAM file
# :: Example Code ::
# CleanSam(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def CleanSam(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, minRam=None, cmd=None):

    #if prjName is None:
    #    prjName = 'my-project-id'

    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'maxulysse/picard'

    if minRam is None:
        minRam = '9'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting job must be given!!\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = "java -Xmx8G -jar /opt/picard/picard.jar CleanSam I=${INFILE} O=${OUTFILE}"

    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--min-ram')
    Args.append(minRam)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process



"""
#------------------------------------------------------------------------------
# FixMateInformation
# :: Example Code ::
# FixMate(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def FixMate(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, minRam=None, cmd=None):

    #if prjName is None:
    #    prjName = 'my-project-id'


    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'maxulysse/picard'

    if minRam is None:
        minRam = '17'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting job must be given!!\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = "java -Xmx16G -Djava.io.tmpdir=`pwd`/tmp -jar /opt/picard/picard.jar FixMateInformation I=${INFILE} O=${OUTFILE}"

    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--min-ram')
    Args.append(minRam)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process


"""
#------------------------------------------------------------------------------
# Building BAM file index
# :: Example Code ::
# BuildBamIndex(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def BuildBamIndex(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, minRam=None, cmd=None):

    #if prjName is None:
    #    prjName = 'my-project-id'


    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'maxulysse/picard'

    if minRam is None:
        minRam = '17'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting job must be given!!\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = "java -Xmx16G -Djava.io.tmpdir=`pwd`/tmp -jar /opt/picard/picard.jar BuildBamIndex I=${INFILE} O=${OUTFILE}"

    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--min-ram')
    Args.append(minRam)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process



"""
#------------------------------------------------------------------------------
# Sorting BAM file
# :: Example Code ::
# SortSam(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def SortSam(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, minRam=None, cmd=None, sorder='coordinate'):

    #if prjName is None:
    #    prjName = 'my-project-id'

    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'maxulysse/picard'

    if minRam is None:
        minRam = '17'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting job must be given!!\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = "java -Xmx16G -Djava.io.tmpdir=`pwd`/tmp -jar /opt/picard/picard.jar SortSam I=${{INFILE}} O=${{OUTFILE}} SORT_ORDER={}".format(sorder)

    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--min-ram')
    Args.append(minRam)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process


"""
#------------------------------------------------------------------------------
# Convert mapped BAM to unmapped BAM file
# :: Example Code ::
# UnmapBam(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""

def UnmapBam(prjName=None, Zones=None, Logs=None, Image=None, inFile=None, outFile=None, scriptPath=None, minRam=None, cmd=None):

    #if prjName is None:
    #    prjName = 'my-project-id'


    if Zones is None:
        Zones = 'us-*'

    #if Logs is None:
    #    Logs = 'gs://my-log'

    if Image is None:
        Image = 'maxulysse/picard'

    if minRam is None:
        minRam = '17'

    assert(not (prjName is None)), "Project ID must be given!!\nExample) my-project-id\n"
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (outFile is None)), "Output file must be given!!\nExample) gs://<bucket>/yyyy.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting job must be given!!\nExample) /local/full/path/script.sh\n"

    if cmd is None:
        cmd = ' '.join(("java -Xmx16G -Djava.io.tmpdir=`pwd`/tmp -jar /opt/picard/picard.jar RevertSam I=${INFILE} O=${OUTFILE}",
                        "SANITIZE=true MAX_DISCARD_FRACTION=0.005 ATTRIBUTE_TO_CLEAR=XT ATTRIBUTE_TO_CLEAR=XN ATTRIBUTE_TO_CLEAR=X0",
                        "ATTRIBUTE_TO_CLEAR=MD ATTRIBUTE_TO_CLEAR=XG ATTRIBUTE_TO_CLEAR=XG ATTRIBUTE_TO_CLEAR=AM ATTRIBUTE_TO_CLEAR=NM",
                        "ATTRIBUTE_TO_CLEAR=SM ATTRIBUTE_TO_CLEAR=XM ATTRIBUTE_TO_CLEAR=XG ATTRIBUTE_TO_CLEAR=XO ATTRIBUTE_TO_CLEAR=X1",
                        "ATTRIBUTE_TO_CLEAR=XA SORT_ORDER=queryname RESTORE_ORIGINAL_QUALITIES=true REMOVE_DUPLICATE_INFORMATION=true REMOVE_ALIGNMENT_INFORMATION=true"))


    #-- Writing Script
    with open(scriptPath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(cmd)


    pgExec = 'dsub'
    Args = []

    #-- job name is same as inFile name
    Args.append('--name')
    Args.append(inFile.split('/')[len(inFile.split('/')) - 1].split('.')[0])

    Args.append('--project')
    Args.append(prjName)

    Args.append('--zones')
    Args.append(Zones)

    Args.append('--logging')
    Args.append(Logs)

    Args.append('--input')
    tmp = 'INFILE={}'.format(inFile)
    Args.append(tmp)

    Args.append('--output')
    tmp = 'OUTFILE={}'.format(outFile)
    Args.append(tmp)

    Args.append('--image')
    Args.append(Image)

    Args.append('--min-ram')
    Args.append(minRam)

    Args.append('--script')
    tmp = "{}".format(scriptPath)
    Args.append(tmp)


    command = [pgExec]
    command.extend(Args)

    process = subprocess.check_output(command)

    #-- Writing process information
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        f.write(str(process).strip())

    return process


"""
#------------------------------------------------------------------------------
# Submit Jobs to GATK Best Practice Pipeline
# :: Example Code ::
# subGenPipe(inFile='gs://cloud-storage-01/example1_DNA.bam', outFile='gs://cloud-storage-02/example1_DNA.head.bam', scriptPath='/local/full/path/script.sh')
# 
# :: TIP ::
# gsutil ls gs://cloud-storage-01 |grep 'bam$' > bamList.txt
# OR
# gsutil ls gs://cloud-storage-01 |grep -H '.*bam$' |sed -e "s|.*\(gs.*$\)|\1|"  > bamList.txt
#------------------------------------------------------------------------------
"""
def subGenPipe(Zones=None, Logs=None, inFile=None, scriptPath=None, GATK_GOOGLE_DIR=None, GATK_OUT_DIR=None, WDL_DIR=None, plPrefix=None):
    if Zones is None:
        Zones = 'us-central1-f'

    #if Logs is None:
    #    Logs = 'gs://my-log'
    
    if plPrefix is None:
        plPrefix = 'PairedEndSingleSampleWf' # pipeline prefix in GATK_GOOGLE_DIR - keep original and may copy to rewrite Json files
        
    assert(not (inFile is None)), "Input file must be given!!\nExample) gs://<bucket>/xxxx.bam\n"
    assert (not (scriptPath is None)), "The path of script file that will be used for submitting jobs must be given!!\nExample) /local/full/path/script.sh\n"
    assert(not (GATK_GOOGLE_DIR is None)), "The path of GATK Best Practices Pipeline templates e.g., /usr/local/broad-prod-wgs-germline-snps-indels\nThis can be downloaded from https://cloud.google.com/genomics/docs/tutorials/gatk"
    assert(not (GATK_OUT_DIR is None)), "Google Cloud Storage to store outputs e.g., gs://my-cloud-storage"
    assert(not (WDL_DIR is None)), "WDL directory found in GATK Best Practices Pipeline examples. e.g., /usr/local/wdl\nThis can be downloaded from https://cloud.google.com/genomics/docs/tutorials/gatk"
    
    sample_json = '{}/PairedEndSingleSampleWf.hg38.inputs.json'.format(GATK_GOOGLE_DIR)
    gatk_json   = '{}/PairedEndSingleSampleWf.gatk4.0.options.json'.format(GATK_GOOGLE_DIR)
    yml_file = '{}/runners/cromwell_on_google/wdl_runner/wdl_pipeline.yaml'.format(WDL_DIR)
    sampleName = inFile.split('/')[-1].split('.')[0]
    
    
    out_json = '{}/{}'.format(scriptPath, sampleName)
    
    
    #try:
    #    os.makedirs(scPath)
    #except OSError:
    #    pass
    

    try:
        os.makedirs(out_json)
    except OSError:
        pass
    
    
    """
    #------------------------------------------------------------------------------
    # Define environment 
    #------------------------------------------------------------------------------
    """
    src = '{}/{}.*'.format(GATK_GOOGLE_DIR, plPrefix)
    for fname in glob.glob(src):
        tmp = fname.split('/')[-1].split('.')
        tmp[0] = sampleName
        dst = '.'.join(tmp)
        dst = '{}/{}'.format(out_json, dst)
        print(dst)
        shutil.copyfile(fname, dst)
    
    
    # Read and update Json for samples
    #------------------------------------------------------------------------------
    with open(sample_json) as f:
        data = json.load(f, object_pairs_hook=OrderedDict)
    
    data['PairedEndSingleSampleWorkflow.base_file_name'] = sampleName
    data['PairedEndSingleSampleWorkflow.final_gvcf_base_name'] = sampleName
    data['PairedEndSingleSampleWorkflow.sample_name'] = sampleName
    data['PairedEndSingleSampleWorkflow.fingerprint_genotypes_file'] = ''
    data['PairedEndSingleSampleWorkflow.flowcell_unmapped_bams'] = [inFile]
        
    out_sJson = '{}/{}.hg38.inputs.json'.format(out_json, sampleName)
    
    with open(out_sJson, 'w') as f:
        json.dump(data, f)
    
    out_gJson = '{}/{}.gatk4.0.options.json'.format(out_json, sampleName)


    # Submit Job
    #------------------------------------------------------------------------------
    pgExec = 'gcloud'
    Args = []
    
    #-- job name is same as inFile name
    Args.append('alpha')
    Args.append('genomics')
    Args.append('pipelines')
    Args.append('run')
    
    Args.append('--pipeline-file')
    Args.append(yml_file)
    
    Args.append('--zones')
    Args.append(Zones)
    
    Args.append('--memory')
    Args.append('5')
    
    Args.append('--logging')
    Args.append(Logs)
    
    Args.append('--inputs-from-file')
    tmp = 'WDL={}/PairedEndSingleSampleWf.gatk4.0.wdl'.format(GATK_GOOGLE_DIR)
    Args.append(tmp)
    
    Args.append('--inputs-from-file')
    tmp = 'WORKFLOW_INPUTS={}'.format(out_sJson)
    Args.append(tmp)
    
    Args.append('--inputs-from-file')
    tmp = 'WORKFLOW_OPTIONS={}'.format(out_gJson)
    Args.append(tmp)
    
    Args.append('--inputs')
    tmp = 'WORKSPACE={}/workspace'.format(GATK_OUT_DIR)
    Args.append(tmp)
    
    Args.append('--inputs')
    tmp = 'OUTPUTS={}'.format(GATK_OUT_DIR)
    Args.append(tmp)
    
    command = [pgExec]
    command.extend(Args)
    
    #process = subprocess.check_output(command)
    process = subprocess.check_output(command, stderr=subprocess.STDOUT)
    
    try:
        jobID = re.search('operations/(.+?)]', str(process)).group(1)
    except AttributeError:
        jobID = ''
        
    #-- Writing process information
    scriptPath = '{}/{}'.format(scriptPath, sampleName)
    procOut = "{}.proc.txt".format(scriptPath)
    with open(procOut, 'w') as f:
        cmt = "gcloud alpha genomics operations describe {} --format='yaml(done, error, metadata.events)'".format(str(jobID).strip())
        f.write(cmt)

    return jobID

