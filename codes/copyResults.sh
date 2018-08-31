#!/usr/bin/env bash

#------------------------------------------------------------------------------
# Purpose     : Write Sentieon Inputs and Output pairs
# Descriptions:
#  - This codes reads the list of inputs and write input output pairs
#
# Start date  : July 6, 2018
# Last update : July 6, 2018
# :USAGE:
# >> bash copyResults.sh gs://my-results /my/local/dir
#------------------------------------------------------------------------------
gsdir=$1; shift
homeDir=$1; shift


ignorDir=("aligned_reads" "worker_logs")                        # list of directories that will be ignored to be copied


#-- extract subdirectory name 1st level 
subdir1=()
while IFS=' ' read -r line || [[ -n "$line" ]]; do
    IFS='/' read -r -a line_arr <<< "$line"                     # read each line  <bam_file> <gs_storage>
    
    numArr=${#line_arr[@]}
    maxIdx=$(( $numArr - 1 ))
    #echo ${line_arr[$maxIdx]}
    subdir1+=(${line_arr[$maxIdx]})

done <<< "$(gsutil ls "$gsdir")"


#-- extract subdirectory name 2nd level 
subdir2=()
cnt=0
for dir1 in ${subdir1[@]}; do                                   # dir1: 1st sub directory name 
#for dir1 in ${subdir1[@]:0:2}; do
    cnt=$(( cnt+1 ))
    printf "\n[ $cnt/${#subdir1[@]} ] processing.... \n"
    
    fpath="$gsdir/$dir1"                                        # fpath: full 1st subdirectory name 
    tmp=($(gsutil ls "$fpath"))
    for fdir2 in ${tmp[@]}; do                                  # full 2nd directory path 
        igflag=false
        for igdir in ${ignorDir[@]}; do
            if [[ "$fdir2" == *"$igdir"* ]]; then
                igflag=true
            fi
        done

        #echo "$igflag"

        if [[ "$igflag" != true ]]; then
            IFS='/' read -r -a dir_arr <<< "$fdir2"             # extract 2nd subdirectory name 
            numArr=${#dir_arr[@]}
            maxIdx=$(( $numArr - 1 ))
            dir2=${dir_arr[$maxIdx]}

            tgPath="$homeDir/$dir1"                             # destination path due to '-r' options  (i.e., gsutil -m cp -r ) the final directory is automatically created

            if [[ -d "$tgPath" ]]; then
                printf "\t - Skipping $tgPath: directory already exists\n"
                #$(gsutil -m cp -r "$fdir2" "$tgPath")
            else
                printf "\t - create directory $tgPath \n"
                $( mkdir -p "$tgPath" )

                printf "\t - copying file to $tgPath \n"
                $(gsutil -m cp -r "$fdir2" "$tgPath")
            fi
            #echo "$dir1"                                        
            #echo "$dir2"
        fi
    done
done
