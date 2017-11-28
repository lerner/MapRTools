#!/bin/bash 

# From the SPOD management node, b139-m1, I run runguts.sh as root:
# clush -Bg b139 /usr/local/bin/runguts.sh  -t $(date +%Y%m%d-%H%M)
# 
# Kill guts with the -k option after you finish runs.  
# If you are just starting a subsequent run, you don't need to explicitly kill guts.  
# The script always does a pkill -x guts before starting up new guts instances.
# clush -Bg b139 /usr/local/bin/runguts.sh  -k
# 
# Confirm that guts is running with the -s option.
# clush -Bg b139 /usr/local/bin/runguts.sh  -s

# Defaults
#TS=$(date +%Y%m%d-%H%M%S)
TS=$(date +%Y%m%d-%H%M) # runs of less than a minute may get overwritten
OUTDIR=/tmp
KILLGUTS=0
PSGUTS=0
#MFSPORTS=$(ps -fu mapr | grep server/mfs | sed -e "s/^.* -p //" | cut -f1 -d' ')
INSTANCES=$(/opt/mapr/server/mrconfig info instances | tail -1 )
HOST=$(hostname)

while getopts "o:t:ksi:m:" OPTION
do
  case $OPTION in 
    t) TS=$OPTARG;;
    o) OUTDIR=$OPTARG;;
    k) KILLGUTS=1;; 	# Kill running guts processes instead of starting
    s) PSGUTS=1;; 	# ps and grep for guts proceses
    i) INSTANCES=$OPTARG;;  # Space separated list of MFS ports
    #m) MFSPORTS=$OPTARG;;  # Space separated list of MFS ports
    #*) echo "Unknown option ${OPTION}.  Exiting";;
  esac
done

if (($KILLGUTS)); then
  pkill -x guts
  exit
fi

if (($PSGUTS)); then
  ps -ef | grep '/opt/mapr/bin/guts' | grep -v grep
  exit
fi

if [[ -z $INSTANCES ]]; then
  echo "No mfs instances running"
  exit
fi

pkill -x guts 2>/dev/null
for PORT in $INSTANCES;  do
  let instanceId=${PORT}-5660
  OUTFILE=$OUTDIR/guts.${HOST}.${instanceId}.${TS}.txt
  echo guts writing to $OUTFILE
  #/opt/mapr/bin/guts time:unix header:none flush:line cache:all db:none cpu:none instance:$instanceId > $OUTFILE &
  /opt/mapr/bin/guts time:all header:none flush:line cache:all db:none cpu:none instance:$instanceId > $OUTFILE &
done
