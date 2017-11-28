#!/bin/bash

# MapR utilities
# . this file then use functions.
# Set mapr_verbose=0 for silent failures

mapr_debug=${mapr_debug:-0}
mapr_verbose=1

if [[ $BASH_SOURCE =~ ^/ ]]; then
  #echo $BASH_SOURCE is absolute
  mapr_utils_scriptpath=$BASH_SOURCE
else
  #echo $BASH_SOURCE is relative
  mapr_utils_scriptpath=$(pwd)/$BASH_SOURCE
fi

mapr_list_utils()      # List these utilities
{
  grep '()' $mapr_utils_scriptpath | grep ^mapr | grep -v grep
}

_mapr_mfs_ports()      # Set MAPR_MFS_PORTS if not already set
{
  if [[ -z $MAPR_MFS_PORTS ]]; then
    export MAPR_MFS_PORTS=$(ps -ef \
                             | grep server/mfs \
			     | grep -v grep \
			     | grep -- -p \
			     | sed -e 's/^.*-p //' \
			     | cut -f1 -d' ' \
                             | sort -n \
			     | tr '\n' ' ' \
			     | sed -e 's/ $//g' \
                           )
  fi
  if [[ $MAPR_MFS_PORTS != "5660" && mapr_verbose -ne 0 ]]; then
    >&2 echo "Warning: Using non-standard MAPR_MFS_PORTS=$MAPR_MFS_PORTS"
  fi
}

# Primary disk for all storage pools on local node

mapr_sp()              # List primary SP devices on node.
{
  _mapr_mfs_ports
  local port
  #/opt/mapr/server/mrconfig sp list | grep path | sed -e "s/.* //"
  for port in $MAPR_MFS_PORTS
  do
    /opt/mapr/server/mrconfig -p $port sp list | grep path | sed -e "s/.* //"
  done
} 

# Non-mapr volumes
mapr_volume()          # List user or all volumes.    optional arg1: "all"
{
  local OPT='-filter "[n!=mapr\.*]"'
  [[ "$1" == "all" ]] && OPT=""
  eval maprcli volume list  -noheader -columns volumename $OPT
} 

_mapr_dev_port()       # Get mfs port for disk device          arg1: dsk device
{
  _mapr_mfs_ports
  local port
  for port in $MAPR_MFS_PORTS
  do
    if /opt/mapr/server/mrconfig -p $port sp list | grep $1 > /dev/null 2>&1 ; then
      echo $port
    fi
  done
}
# Given an SP primary disk, list all rw containers on the SP
mapr_sp_containers()   # List rw containers on an SP.          arg1: SP device
{
  local port
  local MSG="" && (( $mapr_verbose )) && MSG="SP primary device required as argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1
  local DEV=$1

  MSG="" && (( $mapr_verbose )) && MSG="$DEV is not an SP primary device"
  ! mapr_is_sp $DEV && echo $MSG | sed '/^$/d' && return 1

  port=$(_mapr_dev_port $DEV)
  /opt/mapr/server/mrconfig -p $port info containers rw $DEV | tr -d '[:alpha:]:' | sed 's/^ *//'
}

# Given an SP primary disk, list volumes with a rw container on the SP
mapr_sp_volumes()      # List volumes with a container on SP.  arg1: SP device
{
#set -x
  local MSG="" && (( $mapr_verbose )) && MSG="SP primary device required as argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1

  local DEV=$1

  MSG="" && (( $mapr_verbose )) && MSG="$DEV is not an SP primary device"
  ! mapr_is_sp $DEV && echo $MSG | sed '/^$/d' && return 1

  local port=$(_mapr_dev_port $DEV)

  maprcli dump containerinfo \
    -ids \
    $(/opt/mapr/server/mrconfig -p $port info containers rw $DEV \
      | sed -e 's/^RW containers: //' \
      | tr ' ' ','\
     ) \
    -json | grep VolumeName | sort | uniq \
    | tr -d '\t",' \
    | cut -f2 -d':'
}

# Return 0 if $1 is a MapR Storage Pool primary device
mapr_is_sp()           # True if valid SP device.              arg1: SP device
{
  [[ $# -ne 1 ]] && return 1
  local DEV=$1
  mapr_sp | grep $DEV > /dev/null 2>&1 && return 0
  return 1
}

# Return 0 if $1 is a MapR Volume
mapr_is_volume()       # True if valid MapR volume.            arg1: volumename
{
  [[ $# -ne 1 ]] && return 1
  local VN=$1
  maprcli volume info -name $VN > /dev/null 2>&1
  # Need to fix filter
  # maprcli volume info -filter "[volumename==$VN]" > /dev/null 2>&1
}

# Given a volume name, list storage pools that contain a rw container owned by that volume
# Requires clush
old_clush_mapr_volume_sps()      # List SPs with volume's containers.    arg1: volumename
{
  local MSG="" && (( $mapr_verbose )) && MSG="Volume name required as an argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1

  local VN=$1

  MSG="" && (( $mapr_verbose )) && MSG="$VN is not a MapR Volume"
  ! mapr_is_volume $VN && echo $MSG | sed '/^$/d' && return 1

  # NODES=$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ',' | sed -e 's/,$//')
  # SPDEVS=$(/opt/mapr/server/mrconfig sp list | grep path | sed -e "s/.* //")
  # DEVCNTRS=$(/opt/mapr/server/mrconfig info containers rw $DEV | tr -d [:alpha:]\:)
  # VOLCNTRS=$(/opt/mapr/server/mrconfig info containerlist $VN | tr -d [:alpha:])

  local NODES_CSV=$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ',' | sed -e 's/,$//')
  # TBD: Fix to use mrconfig -h host instead of clush
  #sshd_config AcceptEnv accepts passing of several LC_ env vars. Use LC_PAPER to pass volumename
  LC_PAPER=$VN clush -w $NODES_CSV '\
    VN=$LC_PAPER
    VOLCNTRS=$(/opt/mapr/server/mrconfig info containerlist $VN | tr -d [:alpha:])
    SPDEVS=$(/opt/mapr/server/mrconfig sp list | grep path | sed -e "s/.* //")
    for DEV in $SPDEVS; do 
      DEVCNTRS=$(/opt/mapr/server/mrconfig info containers rw $DEV | tr -d [:alpha:]\:)
      for CNTR in $VOLCNTRS; do
        if echo $DEVCNTRS | grep $CNTR > /dev/null 2>&1 ; then
          /opt/mapr/server/mrconfig sp list $DEV | grep path; break
        fi
      done
    done
    ' \
    | sort | cut -f 1,3 -d:
}

# Given a volume name, list storage pools that contain a rw container owned by that volume
mapr_volume_sps()      # List SPs with volume's containers.    arg1: volumename
{
  local MSG="" && (( $mapr_verbose )) && MSG="Volume name required as an argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1

  local VN=$1

  MSG="" && (( $mapr_verbose )) && MSG="$VN is not a MapR Volume"
  ! mapr_is_volume $VN && echo $MSG | sed '/^$/d' && return 1

  # NODES=$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ',' | sed -e 's/,$//')
  # SPDEVS=$(/opt/mapr/server/mrconfig sp list | grep path | sed -e "s/.* //")
  # DEVCNTRS=$(/opt/mapr/server/mrconfig info containers rw $DEV | tr -d [:alpha:]\:)
  # VOLCNTRS=$(/opt/mapr/server/mrconfig info containerlist $VN | tr -d [:alpha:])

  local NODES_SSV="$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ' ' )"
  (( $mapr_debug )) && echo NODES_SSV=$NODES_SSV

  _mapr_mfs_ports
  local node
  local port
  port=$(echo $MAPR_MFS_PORTS | cut -f 1 -d ' ')
  VOLCNTRS=$(/opt/mapr/server/mrconfig -p $port info containerlist $VN | tr -d [:alpha:])
  (( $mapr_debug )) && echo "  VOLCNTRS=$VOLCNTRS"
  for node in $NODES_SSV
  do
    for port in $MAPR_MFS_PORTS
    do
      (( $mapr_debug )) && echo node:port is $node:$port
      SPDEVS=$(/opt/mapr/server/mrconfig -h $node -p $port sp list | grep path | sed -e "s/.* //")
      (( $mapr_debug )) && echo "  SPDEVS=$SPDEVS"
      for DEV in $SPDEVS; do 
        (( $mapr_debug )) && echo "    DEV=$DEV"
        DEVCNTRS=$(/opt/mapr/server/mrconfig -h $node -p $port info containers rw $DEV | tr -d [:alpha:]\:)
        (( $mapr_debug )) && echo "    DEVCNTRS=$DEVCNTRS"
        for CNTR in $VOLCNTRS; do
          (( $mapr_debug )) && echo "      CNTR=$CNTR"
          if echo $DEVCNTRS | grep $CNTR > /dev/null 2>&1 ; then
            echo -n "$node"
	    if [[ $MAPR_MFS_PORTS != "5660" ]]; then
	      echo -n ":$port "
	    fi
            (( $mapr_debug )) && echo mrconfig node:port is $node:$port
            /opt/mapr/server/mrconfig -h $node -p $port sp list $DEV | grep path | cut -f 2 -d':'; break
          fi
        done
      done
    done
  done #\
  #| sort
}

# Given a volume name, list containers for that volume by storage container
mapr_volume_cntrs()    # List volume's containers by SP.       arg1: volumename
{
  local MSG="" && (( $mapr_verbose )) && MSG="Volume name required as an argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1

  local VN=$1

  MSG="" && (( $mapr_verbose )) && MSG="$VN is not a MapR Volume"
  ! mapr_is_volume $VN && echo $MSG | sed '/^$/d' && return 1

  # NODES=$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ',' | sed -e 's/,$//')
  # SPDEVS=$(/opt/mapr/server/mrconfig sp list | grep path | sed -e "s/.* //")
  # DEVCNTRS=$(/opt/mapr/server/mrconfig info containers rw $DEV | tr -d [:alpha:]\:)
  # VOLCNTRS=$(/opt/mapr/server/mrconfig info containerlist $VN | tr -d [:alpha:])

  local NODES_SSV="$(maprcli dump volumenodes -volumename $VN -json | grep VALID | tr -d '\t' | tr -d '"' | cut -f 1 -d : | sort | uniq  | tr '\n' ' ' )"
  (( $mapr_debug )) && echo NODES_SSV=$NODES_SSV

  _mapr_mfs_ports
  local node
  local port
  port=$(echo $MAPR_MFS_PORTS | cut -f 1 -d ' ')
  VOLCNTRS=$(/opt/mapr/server/mrconfig -p $port info containerlist $VN | tr -d [:alpha:])
  (( $mapr_debug )) && echo "  VOLCNTRS=$VOLCNTRS"
  for node in $NODES_SSV
  do
    node=$(getent hosts $node | tr -s ' ' | cut -f2 -d ' ')
    for port in $MAPR_MFS_PORTS
    do
      (( $mapr_debug )) && echo node:port is $node:$port
      SPDEVS=$(/opt/mapr/server/mrconfig -h $node -p $port sp list | grep path | sed -e "s/.* //")
      (( $mapr_debug )) && echo "  SPDEVS=$SPDEVS"
      for DEV in $SPDEVS; do 
        (( $mapr_debug )) && echo "    DEV=$DEV"
        DEVCNTRS=$(/opt/mapr/server/mrconfig -h $node -p $port info containers rw $DEV | tr -d [:alpha:]\:)
        (( $mapr_debug )) && echo "    DEVCNTRS=$DEVCNTRS"
	CURRENTSP="$(/opt/mapr/server/mrconfig -h $node -p $port sp list | grep -w $DEV | cut -f1 -d',')"
        (( $mapr_debug )) && echo "    CURRENTSP=%$CURRENTSP%"
	FIRSTTIME=1
	CNTRLIST=""
        for CNTR in $VOLCNTRS; do
          (( $mapr_debug )) && echo "      CNTR=$CNTR"
          if echo $DEVCNTRS | grep $CNTR > /dev/null 2>&1 ; then
	    if (( $FIRSTTIME )); then
	      FIRSTTIME=0
              echo -n "$node"
	      if [[ $MAPR_MFS_PORTS != "5660" ]]; then
	        echo -n ":$port "
              else
                echo -n " "
	      fi
	      echo -n "$CURRENTSP - "
	    fi
	    CNTRLIST="$CNTRLIST $CNTR"
          fi
        done
	echo $CNTRLIST
      done
    done
  done #\
  #| sort
}

# Given a volume name, list storage pools that contain a rw container owned by that volume
# Requires clush
#mapr_volume_sps()      # List SPs with volume's containers.    arg1: volumename
mapr_volume_repl()     # Show volume expected/actual repl.     arg1: volumename
{
  local MSG="" && (( $mapr_verbose )) && MSG="Volume name required as an argument"
  [[ $# -ne 1 ]] && echo $MSG | sed '/^$/d' && return 1

  local VN=$1

  # Sample filter volumes starting with iq or ap
  # mapr_volume_repl 'iq*]or[volumename==ap*'
  # MSG="" && (( $mapr_verbose )) && MSG="$VN is not a MapR Volume"
  # ! mapr_is_volume $VN && echo $MSG | sed '/^$/d' && return 1

#VN='iq*';
  local NL
  local REPLINE=0
  local REPL
#echo VN=$VN
  printf "%-32s%5s%5s%5s%5s%5s%5s%5s\n" volumename numr minr 0x 1x 2x 3x 4x
#volumename,actualreplication,drf,mrf
  maprcli volume list -columns volumename,actualreplication,numreplicas,minreplicas -filter "[volumename==$VN]" -json | tr -d '",' \
  | while read NL; do 
#echo $NL
      if [[ $NL =~ ^volumename ]]; then 
        printf "%-32s" ${NL##volumename:} 
	continue; 
      fi
      if [[ $NL =~ ^numreplicas ]]; then 
        local NUMR=$(echo $NL | cut -f 2 -d ':')
	continue
      fi 
      if [[ $NL =~ ^minreplicas ]]; then 
        local MINR=$(echo $NL | cut -f 2 -d ':')
	continue
      fi 
      if [[ $NL =~ ^actualreplication ]]; then 
        REPLINE=1 
	continue
      fi 
      if (($REPLINE)); then 
        REPL[$REPLINE]=$NL
	let REPLINE+=1
#echo REPLINE=$REPLINE
        if [[ $REPLINE == 6 ]]; then 
	  printf "%5d%5d%5d%5d%5d%5d%5d\n" $NUMR $MINR ${REPL[1]} ${REPL[2]} ${REPL[3]} ${REPL[4]} ${REPL[5]}
	  REPLINE=0
	fi 
	continue
      fi
    done
}

