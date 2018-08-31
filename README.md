# BAM to VCF
Although Google Cloud Genomics and Sentieon pipelines support robust tools to obtain high quality VCFs from BAM files, obtaining VCF files from these pipelines is not a trivial task.  The problem is made worse when a BAM file has missing information in the header or an incomplete file structure to utilize GATK pipelines. The problem gets more complicated and challenging when the number of BAM files that needs to be processed is too large to rely on manual file management techniques and all of the files need to be transferred in and out of Google Cloud Storage. Therefore, we have implemented pipelines and source codes for producing VCF files from BAM files by utilizing Google Cloud Genomics and Sentieon pipelines.

## 1. Preprocessing BAM 
![Preprocessing for BAM files](images/BAM_step1.png)

## 2. Filtering Germline Mutations
![Filtering Germline Mutations](images/filters.png)

## 3. Unmapping BAM and Job Submission 
![Unmapping BAM and Job Submission](images/BAM_step2.png)


# Prerequisite for Google Genomics Pipelines
`https://cloud.google.com/genomics/docs/tutorials/gatk`

### `/codes`
0. `dsub.py`          : Bam Preparation Tools for Genomic analysis - collections of a function to be used in other codes. **Variable 'PrjName' and 'Logs' in `dsub.py` MUST be redefined with your account information**

1. `addPL.py`         : Adding PL flag in BAM header
2. `cleanSam.py`      : Remove errors in BAM file
3. `fixMate.py`       : Correct Mate Pair errors in BAM file
4. `sortBam.py`       : Sort BAM files based on coordinates
5. `buildIndex.py`    : Build index files xxxx.bam.bai
6. `cmpFiles.py`      : Compare two lists and write same and differences - to check if output files are all produced without out error by checking the input and output file names
7. `InputSentieon.py` : Write input lists to submit Sentieon jobs
8. `runGenPipe.py`    : Submitting Genomic Pipeline jobs 
8. `copyResults.sh`   : Copy final results files into local disk

### `/batch`
 0. `submit_batch.sh` : Submitting Sentieon jobs (Official release [https://github.com/Sentieon/sentieon-google-genomics/tree/master/batch](https://github.com/Sentieon/sentieon-google-genomics/tree/master/batch)). **This code is developed by '_Don Freed_', Bioinformatics Scientist in Sentieon**
	
```
	$ bash submit_batch.sh batch.json batch_bam.tsv
```
 1. `somatic.json`    : configuration for somatic analysis
 2. `germline.json`   : configuration for germline analysis
 3. `batch_fastq.tsv` : an example of input lists (**column headers are used as variable** when submitting a job)
 4. `somatic_bam.tsv` : template of input lists for Sentieon somatic sentieon analysis
 5. `germline_bam.tsv`: template of input lists for Sentieon germline analysis



## Prerequisite for Sentieon Pipelines
### 1. Install dsub
```
	$ git clone [https://github.com/googlegenomics/dsub](https://github.com/googlegenomics/dsub)
	$ python setup.py install
```
### 2. Install sentieon scripts
```
	$ git clone [https://github.com/sentieon/sentieon-google-genomics.git](https://github.com/sentieon/sentieon-google-genomics.git)
	$ cd sentieon-google-genomics
```



## Prerequisite for Google Genomics Pipelines
`https://cloud.google.com/genomics/docs/tutorials/gatk`

### 1. Install Could SDK
   `https://cloud.google.com/sdk/docs/`

### 2. Update and install gcloud components:
```
    $ gcloud components update &&
    $ gcloud components install alpha
```
### 3. Install git to download the required files.
```
    $ git clone https://github.com/openwdl/wdl.git
    $ git clone https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels.git
```

# Sentieon Work Flow

### Dockers for using Picard and Samtools
 - [https://quay.io/search?q=gatk](https://quay.io/search?q=gatk)
 - [https://cloud.google.com/compute/docs/images](https://cloud.google.com/compute/docs/images)
 - [https://hub.docker.com/](https://hub.docker.com/) 

### 1. Do & Test Sentieon with above scripts
#### Match reference genome 
- all reference 'vcf' files need to be compressed with 'bcftools'
- Let's assume a BMA has missing 'PL' information in the header. `/codes/addPL.py` can be used (** This code can be modified to correct or fill out other information) 
- Follow GATK preprocessing steps above
  1) CleanSam - Cleans the provided SAM/BAM, soft-clipping beyond-end-of-reference alignments and setting MAPQ to 0 for unmapped reads
```
	java -Xmx8G -jar ./picard.jar CleanSam \
	I=DNA.backward.bam \
	O=DNA.backward.clean.bam
```

  2) ValidateSamFile
 ```
	java -Xmx8G -jar ./picard.jar ValidateSamFile \
	I=DNA.backward.clean.bam \
	MODE=SUMMARY
```

  3) FixMateInformation if Mate related errors are shown in ValidateSamFile (e.g., ERROR:MISMATCH_FLAG_MATE_NEG_STRAND, ERROR:MISMATCH_FLAG_MATE_UNMAPPED)
```
	java -Xmx8G -jar /Users/jjeong/local/programs/picard/picard.jar FixMateInformation \
	I=DNA.backward.clean.bam \
	O=DNA.backward.clean.FixMate.bam
```

  
### 2. Check dsub status and cancel jobs

  1) To check the status, run:
  ```
  	$ dstat --project my-project-id --jobs '< JOB ID >' --status '*'
  ```

  2) To cancel the job, run:
  ```
	$ ddel --project my-project-id --jobs '< JOB ID >'
  ```

  3) list of jobs
  ```
	$ dstat --project 'my-project-id'
  ```


### 3. Read dsub error log that is defined in dsub script (e.g., gs://my-logs/dsub )


## An example pipeline for Sentieon Haplotype
**Variable 'PrjName' and 'Logs' in `dsub.py` MUST be redefined with your account information**
### Pipeline steps
1) `addPL.py`
2) `cleanSam.py`
3) `fixMate.py`
4) `sortBam.py`
5) `buildIndex.py`
6) `InputSentieon.py`
7) batch bash /batch/submit_batch.sh /batch/germline.json /batch/germline_bam.tsv


### Prerequsite
	https://cloud.google.com/genomics/docs/tutorials/sentieon

#### 1. Write the list of current GCP cloud storage and store them into files
```
	$ gsutil ls gs://gatk-bam | grep 'bam$' > gatk-bam_org.txt
	$ gsutil ls gs://vcf-to-bam-bam | grep 'bam$' > vcf-to-bam-bam_org.txt
	$ gsutil ls gs://vcf-to-bam-bam2 | grep 'bam$' > vcf-to-bam-bam2_org.txt
	$ gsutil ls gs://vcf-to-bam-bam3 | grep 'bam$' > vcf-to-bam-bam3_org.txt
	$ gsutil ls gs://vcf-to-bam-bam4 | grep 'bam$' > vcf-to-bam-bam4_org.txt
```
#### 2. Sync UK Object Storage and GCP cloud storage
		$ rclone sync -v OBJS:/my/BAMs GS:gatk-bam

#### 3-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://gatk-bam | grep 'bam$' > /output_dir/gatk-bam_new.txt
	$ python /codes/cmpFiles.py -r /output_dir/gatk-bam_new.txt -t gatk-bam_org.txt -o <output_dir>
```
#### 3-2. Add platform information with 'missing.txt' file obtained from STEP 3-1
```
	$ python addPL.py -p <my-project-id> -i <output_dir/missing.txt> -o gs://vcf-to-bam-bam -s /output_dir/addPL
	$ dstat --project my-project-id
```
#### 4-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam | grep 'bam$' > /output_dir/vcf-to-bam-bam_new.txt
	$ python /codes/cmpFiles.py -r vcf-to-bam-bam_new.txt -t vcf-to-bam-bam2_new.txt -o /output_dir/vcf-to-bam-bam2
```

#### 4-2. Clean BAM file 
```
	$ python /codes/cleanSam.py -i /output/dir/vcf-to-bam-bam2/missing.txt -o gs://vcf-to-bam-bam2 -s /output_dir/cleanSAM
	$ dstat --project my-project-id
```

#### 5-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam2 | grep 'bam$' > /output_dir/vcf-to-bam-bam2_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam2_new.txt -t vcf-to-bam-bam2_org.txt -o /output_dir/vcf-to-bam-bam2
```

#### 5-2. ** Run dsub/fixMate.py
```
	$ python fixMate.py -p <my-project-id> -i /output_dir/vcf-to-bam-bam2/missing.txt -o gs://vcf-to-bam-bam3 -s /output_dir/fixMate
	$ dstat --project my-project-id
```
#### 6-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam3 | grep 'bam$' > /output_dir/vcf-to-bam-bam3_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam3_new.txt -t vcf-to-bam-bam3_org.txt -o /output_dir/vcf-to-bam-bam3
```
#### 6-2. ** Run dsub/sortBam.py
```
	$ python sortBam.py -p <my-project-id> -i /output_dir/vcf-to-bam-bam3/missing.txt -o gs://vcf-to-bam-bam4 -s /output_dir/sortBam
	$ dstat --project my-project-id
```
#### 6-3. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam4 | grep 'bam$' > /output_dir/vcf-to-bam-bam4_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam4_new.txt -t vcf-to-bam-bam4_org.txt -o /output_dir/vcf-to-bam-bam4
```
#### 6-4. ** Run dsub/buildIndex.py
```
	$ python buildBamIndex.py -p <my-project-id> -i /output_dir/missing.txt -o gs://vcf-to-bam-bam4 -s /output_dir/buildIdx
	$ dstat --project my-project-id
```
#### 6-5. ** Run dsub/InputSentieon.py
```
	$ python InputSentieon.py -p <my-project-id> -i /output_dir/missing.txt -o /batch/bam4.tsv -s gs://jc-sentieon
```
#### 6-6. ** Run batch/submit_batch.sh
```
	$ bash /batch/submit_batch.sh ./batch.json /batch/bam4.tsv
```

Contents of ```/batch/batch.json```
```
{
  "REF": "gs://jc-references/hg19/hg19_ucsc/hg19_ucsc.fa",
  "BQSR_SITES": "gs://jc-references/broad/hg19/Mills_and_1000G_gold_standard.indels.hg19.sites.vcf,gs://jc-references/broad/hg19/1000G_phase1.indels.hg19.sites.vcf,gs://jc-references/broad/hg19/dbsnp_138.hg19.vcf",
  "DBSNP": "gs://jc-references/broad/hg19/dbsnp_138.hg19.vcf",
  "READGROUP": "@RG\\tID:my-rgid-1\\tSM:my-sm\\tPL:ILLUMINA,@RG\\tID:my-rgid-2\\tSM:my-sm\\tPL:ILLUMINA",
  "DEDUP": "markdup",
  "ZONES": "us-central1-a,us-central1-b,us-central1-c,us-central1-f",
  "PROJECT_ID": "my-project-id"
}
```
#### 7. Copy results into local drive 
Destination directory will be automatically created if it does not exist. 
Files will not be downloaded if the same name of local directory already exists.
```
	$ bash copyResults.sh gs://jc-sentieon /destination_dir/FMVCF
```



## An example pipeline for Sentieon MuTec2
**Variable 'PrjName' and 'Logs' in `dsub.py` MUST be redefined with your account information**
### Pipeline steps
1) `addPL.py`
2) `cleanSam.py`
3) `fixMate.py`
4) `sortBam.py`
5) `buildIndex.py`
6) `InputSentieon.py`
7) batch bash /batch/submit_batch.sh /batch/somatic.json /batch/somatic.tsv

#### 1. Get Somatic Mutation VCF from COSMIC DB (Catalogue Of Somatic Mutations In Cancer) - coding and non-coding mutations
```
	1) Log in https://cancer.sanger.ac.uk/cosmic
	2) Data > Downloads
	3) Genome Version > GRCh37/h19, CRCh38/hg38 
	4) Choose Coding/noncoding Mutations
	5) 	$ echo "email@example.com:mycosmicpassword" | base64
		  amplb25nQGtjci51a3kuZWR1OnFydXF0YzAxCg==
		$ curl -H "Authorization: Basic amplb25nQGtjci51a3kuZWR1OnFydXF0YzAxCg==" https://cancer.sanger.ac.uk/cosmic/file_download/GRCh37/cosmic/v85/VCF/CosmicCodingMuts.vcf.gz
			<URL_provided> - it will be expired 
	6) Download data via provided URL
		$ curl --output ./CosmicCodingMuts.vcf.gz <URL_provided>

	7) Upload data into cloud storage
		$ gsutil cp CosmicCodingMuts.vcf.gz gs://jc-references/cosmic
```

#### 2. Get Panel of Normal VCF
```
	1) Panel of Normal : remove variants associated with this VCF file 
		- download data from The Exome Aggregation Consortium (ExAC) http://exac.broadinstitute.org/
		- VCF is based on GRCh37/h19, Liftvoer GRCh38 version is only in Google Cloud Storage link can be found via http://exac.broadinstitute.org/downloads

	2) DB SNP - DB SNPs are likely to be a germline since it is based on 1000 Genome projects and the purpose of this project was to figure out the nature of genetic diversity across different countries
		Download from ftp://ftp.ncbi.nih.gov/snp/organisms/
```

#### 3. Submit with BASH script 
```
	1) Change the column name 'BAM' to 'TUMOR_BAM' in '.tsv' file
	2) In batch.json file, add 'PIPELINE' variable 
		{
		  "REF": "gs://jc-references/hg19/hg19_ucsc/hg19_ucsc.fa",
		  "DBSNP": "gs://jc-references/broad/hg19/dbsnp_138.hg19.vcf",
		  "ZONES": "us-central1-a,us-central1-b,us-central1-c,us-central1-f",
		  "PROJECT_ID": "my-project-id”, 
		  "PIPELINE": "TNscope"
		}
 ```
 






# Google Genomics Work Flow
**Variable 'PrjName' and 'Logs' in `dsub.py` MUST be redefined with your account information**
### Pipeline steps
1) `addPL.py`
2) `cleanSam.py`
3) `fixMate.py`
4) `sortBam.py`
5) `buildIndex.py`
6) `InputGCGP.py`
7) `submitGCGP.py`

#### 1. Write the list of current GCP cloud storage and store them into files
```
	$ gsutil ls gs://gatk-bam | grep 'bam$' > gatk-bam_org.txt
	$ gsutil ls gs://vcf-to-bam-bam | grep 'bam$' > vcf-to-bam-bam_org.txt
	$ gsutil ls gs://vcf-to-bam-bam2 | grep 'bam$' > vcf-to-bam-bam2_org.txt
	$ gsutil ls gs://vcf-to-bam-bam3 | grep 'bam$' > vcf-to-bam-bam3_org.txt
	$ gsutil ls gs://vcf-to-bam-bam4 | grep 'bam$' > vcf-to-bam-bam4_org.txt
```
#### 2. Sync Object Storage and GCP cloud storage using `rclone`
		$ rclone sync -v OBJS:/my/BAMs GS:gatk-bam

#### 3-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://gatk-bam | grep 'bam$' > /output_dir/gatk-bam_new.txt
	$ python /codes/cmpFiles.py -r /output_dir/gatk-bam_new.txt -t gatk-bam_org.txt -o <output_dir>
```
#### 3-2. Add platform information with 'missing.txt' file obtained from STEP 3-1
```
	$ python addPL.py -p <my-project-id> -i <output_dir/missing.txt> -o gs://vcf-to-bam-bam -s /output_dir/addPL
	$ dstat --project my-project-id
```
#### 4-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam | grep 'bam$' > /output_dir/vcf-to-bam-bam_new.txt
	$ python /codes/cmpFiles.py -r vcf-to-bam-bam_new.txt -t vcf-to-bam-bam2_new.txt -o /output_dir/vcf-to-bam-bam2
```

#### 4-2. Clean BAM file 
```
	$ python /codes/cleanSam.py -p <my-project-id> -i /output/dir/vcf-to-bam-bam2/missing.txt -o gs://vcf-to-bam-bam2 -s /output_dir/cleanSAM
	$ dstat --project my-project-id
```

#### 5-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam2 | grep 'bam$' > /output_dir/vcf-to-bam-bam2_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam2_new.txt -t vcf-to-bam-bam2_org.txt -o /output_dir/vcf-to-bam-bam2
```

#### 5-2. Fix Mate Pair errors
```
	$ python fixMate.py -p <my-project-id> -i /output_dir/vcf-to-bam-bam2/missing.txt -o gs://vcf-to-bam-bam3 -s /output_dir/fixMate
	$ dstat --project my-project-id
```
#### 6-1. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam3 | grep 'bam$' > /output_dir/vcf-to-bam-bam3_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam3_new.txt -t vcf-to-bam-bam3_org.txt -o /output_dir/vcf-to-bam-bam3
```
#### 6-2. Sorting Reads in BAM files
```
	$ python sortBam.py -p <my-project-id> -i /output_dir/vcf-to-bam-bam3/missing.txt -o gs://vcf-to-bam-bam4 -s /output_dir/sortBam
	$ dstat --project my-project-id
```
#### 6-3. Make the list of current GCP cloud storage and find missing/candidates BAM files
```
	$ gsutil ls gs://vcf-to-bam-bam4 | grep 'bam$' > /output_dir/vcf-to-bam-bam4_new.txt
	$ python cmpFiles.py -r /output_dir/vcf-to-bam-bam4_new.txt -t vcf-to-bam-bam4_org.txt -o /output_dir/vcf-to-bam-bam4
```
#### 6-4. Build BAM file index
```
	$ python buildBamIndex.py -p <my-project-id> -i /output_dir/missing.txt -o gs://vcf-to-bam-bam4 -s /output_dir/buildIdx
	$ dstat --project my-project-id
```

#### 7-1. Build unmapped BAM files
```
	$ python unmapBam.py -p <my-project-id> -i /output_dir/vcf-to-bam-bam4_new.txt -o gs://vcf-to-bam-unmapbam -s /output_dir/unmapBam
	$ dstat --project my-project-id
```

#### 7-2 Run Genomic Cloud Pipelines
**\#\#\_COMMENT2 in PairedEndSingleSampleWf.hg38.inputs.json MUST be redefined**
```
	$GATK_OUTPUT_DIR = GS storage (e.g., gs://jc-gatk-out)
	$GATK_GOOGLE_DIR = Path of GCP Genomic pipeline local path (e.g., /broad-prod-wgs-germline-snps-indels)
```

Google Genomics Pipeline submit command [https://cloud.google.com/genomics/docs/quickstart](https://cloud.google.com/genomics/docs/quickstart)
```
	gcloud alpha genomics pipelines run \
     --pipeline-file wdl/runners/cromwell_on_google/wdl_runner/wdl_pipeline.yaml \
     --zones us-central1-f \
     --memory 5 \
     --logging "${GATK_OUTPUT_DIR}/logging" \
     --inputs-from-file WDL="${GATK_GOOGLE_DIR}/PairedEndSingleSampleWf.gatk4.0.wdl" \
     --inputs-from-file WORKFLOW_INPUTS="${GATK_GOOGLE_DIR}/PairedEndSingleSampleWf.hg38.inputs.json" \
     --inputs-from-file WORKFLOW_OPTIONS="${GATK_GOOGLE_DIR}/PairedEndSingleSampleWf.gatk4.0.options.json" \
     --inputs WORKSPACE="${GATK_OUTPUT_DIR}/workspace" \
     --inputs OUTPUTS="${GATK_OUTPUT_DIR}/1030_FM_DNA"
```

Check Job Status
```
	$ gcloud alpha genomics operations describe <OPERATION_ID> --format='yaml(done, error, metadata.events)'
```

# [Under Development](#) : Extracting Somatic Mutations

**Due to the shortage of funding, this pipeline has not been implemented and tested**


#### 1. [Under Development](#) : Filtering the variants with following files
```
1) Removing any variants from the VCF that are also present in dbSNP or ExAC (Exome Aggregation Consortium)
EXCETP BRCA1/2 and TP53 - these two MUST be kept
2) Keep variants listed in COSMIC 
```

#### 2. VCF Searching tools
```
1) VCF tools : http://vcftools.sourceforge.net/index.html
2) PYVCF : https://pyvcf.readthedocs.io/en/latest/index.html
```
