#!/bin/bash 

# ~/chunkdistr.sh '/mapr/b139/pio/fio.r3.high_throughput/b139-1[1-4]p1.[0-9].* /mapr/b139/pio/fio.r3.high_throughput/b139-1[1-4]p1.1[0-5].* ' 
# Also started chunkdistr.py

FILELIST=$1	# File containing the list of mfs files to look at with hadoop mfs -lss or just the dirs/files
# rm 4gwfilelist.4vol; for gw in 1 2 3 4 ; do find /mapr/NFS28/dd/g${gw}.r3/ucs-node31.perf.lab | grep 'dd\.' | tr '.' ' ' | sort -n  -k5 | tr ' ' '.' >> 4gwfilelist.4vol ; done
# FILELIST=4gwfilelist.4vol
REPLICA=${2:-1,2,3}	# Default is to just three replicas
#REPLICA=2,3	 # Show just 2nd and 3rd replica
STRIPSTR=_mapr.m[0-9]


dir_chunk_distribution_by_node()
{
for FILEEXPR in "${FILELIST_ARR[*]}"
do
  echo $FILEEXPR 1>&2
  hadoop mfs -lss $FILEEXPR | \
  #hadoop mfs -ls $FILELIST | \
  # 1. Remove name container from count
  grep -v ' p '  | \
  
  # 1a. Remove filename, Found, and Total lines
  grep -v '/mapr' | grep -v '^Found' | grep -v 'Total Disk Blocks' | \

#cat;exit
    # 2. Chomp tabs and spaces to a single space
    sed -e 's/[[:space:]]*/ /' | \
    tr -s ' ' | \

    # 3. Strip extraneous hostname info down to nodenum:port
    #sed -e 's/ucs-node//g' -e 's/.perf.lab//g' -e 's/_mapr.//g' | \
    #sed -e 's/_mapr.m[0-9]//g' | \
    sed -e "s/$STRIPSTR//g" | \

#cat;exit
    # 4. Strip cid.fid.epoch and disk block count to leave just replica location info
    #cut -f 4-6 -d ' ' | \
    cut -f 5-7 -d ' ' | \

#cat;exit
    # 5. Show just $REPLICAth replica
    cut -f $REPLICA -d ' '  | \

    # 6. Put each chunk and replica on a separate line
    #tr ' ' '\n' | \

    # Strip port
    #sed -e 's/:[0-9]*//g' | \

    # LAST
    cat
done | \
    sort | \
    uniq -c | \

    # For uniq lines with frequency count, sort
    sort -n | \
# LAST
cat
}

chunk_distribution_by_node()
{
for FILE in $(cat $FILELIST) 
do 
  # echo $FILE
  hadoop mfs -lss $FILE | \

  # 1. Remove name container from count
  grep -v ' p '  | \
  
  # 1a. Remove filename, Found, and Total lines
  grep -v '/mapr' | grep -v '^Found' | grep -v 'Total Disk Blocks' | \
    # 1. Skip the header lines
    # tail --lines +3 | \

#cat;break
    # 2. Chomp tabs and spaces to a single space
    sed -e 's/[[:space:]]*/ /' | \
    tr -s ' ' | \

    # 3. Strip extraneous hostname info down to nodenum:port
    #sed -e 's/ucs-node//g' -e 's/.perf.lab//g' -e 's/_mapr.//g' | \
    sed -e "s/$STRIPSTR//g" | \

    # 4. Strip cid.fid.epoch and disk block count to leave just replica location info
    cut -f 5-7 -d ' ' | \

    # 5. Show just $REPLICAth replica
    cut -f $REPLICA -d ' '  | \

    # Strip port
    #sed -e 's/:[0-9]*//g' | \

    # LAST
    cat
done | \

    sort | \
    uniq -c | \

    # For uniq lines with frequency count, sort
    sort -n | \
# LAST
cat
}


# Use an NFS mount path to a hadoop directory for FILELIST to get entire directory
# Much faster than looping through all the files individually

FILELIST_ARR=("$FILELIST")
FILELIST_CNT=${#FILELIST_ARR[@]}
#echo $FILELIST_CNT
#echo ${FILELIST_ARR[*]}
for FILEEXPR in "${FILELIST_ARR[*]}"
do
  if [[ -d $FILELIST || $FILELIST =~ ^/mapr ]] ; then
    dir_chunk_distribution_by_node
    break
  else
    #first_chunk_distribution_by_mfs
    #first_chunk_distribution_by_node
    chunk_distribution_by_node
    break
  fi
done

