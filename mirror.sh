#!/bin/bash 
# Author: Andy Lerner
# Update History:
#  13Nov2015
#  13Oct2017    Source volume is now mounted at location without appended number.
#               Added sleep command with -s option to specify a number of
#                 seconds to sleep.  This is useful when specifying multiple
#                 -a commands and you want to sleep between them.
#               Added -w option for check command to wait until 100% complete
#  14Oct2017    Combined action that end with _volumes in case statement.
#               Fix -M option default description in usage function
#  
# If BASEDIR is not specified, volumes are created at root level
# which requires root to mount the volumes.
# Non root user can create volumes if BASEDIR is specified at a
# location where the user has permission to write.
# works if root creates volumes, sybaseiq promotes volumes.
# sybaseiq MUST have fc on the CLUSTER (See 18775)
#
# All examples below are run as the sybaseiq user.  
#
# Ensure this user has fc priv:
#   $ maprcli acl show -type cluster -user sybaseiq
#   Principal      Allowed actions      
#   User sybaseiq  [login, ss, cv, fc]
#
# If fc privilege is not listed, run the following command as root or mapr:
#   # maprcli acl edit -type cluster -user sybaseiq:fc
#
# Examples:
# 1. Create a standard volume with 3 mirror volumes mounted under /user/sybaseiq
#    ./mirror.sh -v -N iqvol -a create -b /user/sybaseiq -c 3
# 2. List the volumes.  First volume is always the RW volume, others are mirrors
#    ./mirror.sh -v -N iqvol -a list
# 3. Start mirror operation to iqvol1 mirror volume 
#    ./mirror.sh -v -N iqvol -a start -m 1
# 4. Check to see if the mirror operation to iqvol1 has completed and
#    delete mirror volume snapshots if it has completed.
#    ./mirror.sh -v -N iqvol -a check -m 1
# 5. Start mirror operation to iqvol2 mirror volume 
#    ./mirror.sh -v -N iqvol -a start -m 2
# 6. Revert to mirror iqvol1 data promoting it to be a RW volume
#    ./mirror.sh -v -N iqvol -a promote -m 1
# 7. List the volumes and see that iqvol1 is now listed first as the RW volume.
#    The other volumes, including iqvol0 are now mirror volumes for iqvol1.
#    ./mirror.sh -v -N iqvol -a list
# 8. Start mirror operation to iqvol0 which is now a mirror volume
#    following iqvol1's promotion from mirror to RW volume.
#    ./mirror.sh -v -N iqvol -a start -m 0
# 9. Create a standard volume with 3 mirrors mounted under /apps/hana/data.
#    The volumes are named hana.data.AML[0-3].
#    The source volume is mounted at /apps/hana/data/AML
#    The mirrors are mounted at /apps/hana/data/AML[0-3]
#    ./mirror.sh -v -N hana.data.AML -a create -M AML -b /apps/hana/data -c 3
#10. Loop through the three mirrors starting a mirror, waiting for completion,
#    unmounting all volumes, promoting the new mirror, then remounting all
#    volumes with the promoted mirror at /apps/hana/data/AML
#    for i in {1..3}; do
#      date | hadoop fs -put - /apps/hana/data/AML/file.$(date +%Y%m%d%H%M)
#      hadoop fs -ls /apps/hana/data/AML
#      ./mirror.sh -v -N hana.data.AML -m ${i} -a start -s 3 -a check -w -a unmount -a promote -a mount
#    done
#
# Only the superusers mapr and root can create a mountpoint at the top level of 
# the Hadoop filesystem.  To create volumes at the top level of the Hadoop
# filesystem, run this script as one of those two users and do not
# specify the -b option (or specify "/") to create the volumes at the top level.
# Specify the -u and -g options for the user and group that will be using the mirrors,
# sybaseiq for the examples above.  Remove the volumes with -a remove will also
# require root or mapr, but all other actions can be performed by standard users
# with fc privilege on the cluster as specified above.
#
ACTIONS=""
MIRRORS=-1	# Initialize to -1
BASEDIR=""
ADMINUSER=$(id -un)	# Default to current effective user
ADMINGROUP=$(id -gn)	# Default to current effective group
#SUDOADMIN="sudo -u $ADMINUSER"
SUDOADMIN=""
QUOTA=0		# No limit if Quota=0
CLUSTERNAME=""
#ECHO=/bin/echo

ECHO=""
VERBOSE=0
PREVIEW=0
#PERMS=777
PERMS=750
WAITCOMPLETE=false
SLEEPSECS=5

SCRIPTNAME=$(basename $0)

usage()
{
  echo -n "Usage: "
  echo  "$SCRIPTNAME "
  echo  "  [-p]                 Preview"
  echo  "  [-v]                 Verbose"
  echo  "  -N <namebase>        Volume name base (will have number appended)"
  echo  " "
  echo  " actions: (multiple -a options will execute actions in order given)"
  echo  "  -a [ list | remove | create | promote | [un]mount | start | check | sleep ]"
  echo  " "
  echo  " create options:"
  echo  "  -b <basedir>         Directory that created volumes will be mounted under"
  echo  "  -c <mirror count>    Number of mirror volumes to create"
  echo  "  [-u <admin user>]    Administrative user for volume"
  echo  "  [-g <admin group>]   Administrative group for volume"
  echo  "  [-M <mountbase>]     Volume mount point base (default to namebase)"
  echo  "  [-q <quota>]         Volume quota (default to no quota)"
  echo  " "
  echo  " promote, start, and check options:"
  echo  "  -m <volume_number>   Number of mirror volume to promote, start or check status "
  echo  "  -w                   Continue checking until 100% complete"
  echo  " "
  echo  " sleep option:"
  echo  "  -s <sleep_seconds>   Number of seconds to sleep "
}

warn()
{
  echo "$SCRIPTNAME: Warning: $@"
}

err_exit() {
  echo "$SCRIPTNAME: Error: $@"
  exit 1
}

backtrace()
{
   #echo "Backtrace is:"
   local i=0
   while caller $i
   do
      i=$((i+1))
   done
}

while getopts "a:b:c:C:g:m:M:N:ps:u:vwh" OPTION
do
  case $OPTION in
    a) ACTIONS="$ACTIONS $OPTARG" 
       ;;
    b) BASEDIR=$OPTARG 
       if [[ $BASEDIR == "/" ]]; then BASEDIR=""; fi
       ;;
    c) MIRRORS=$OPTARG ;;
    C) CLUSTERNAME=$OPTARG ;;
    g) ADMINGROUP=$OPTARG ;;
    m) MIRRORIDX=$OPTARG ;;
    M) VOLDIRBASE=$OPTARG ;;
    N) VOLNAMEBASE=$OPTARG ;;
    p) PREVIEW=1 ;;
    s) SLEEPSECS=$OPTARG ;;
    u) ADMINUSER=$OPTARG ;;
    v) VERBOSE=1 ;;
    w) WAITCOMPLETE=true ;;
    h)
      usage
      exit 1 ;;
    *)
      usage
      exit 1 ;;
  esac
done

exec_cmd() {
  if (($VERBOSE|$PREVIEW)); then
    echo "$@"
  fi
  if ((! $PREVIEW)); then
    $@
    return $?
  fi
}


if (($VERBOSE)); then echo $ACTIONS; fi

check_prerequisites() {
  # User must have access to the cluster and be able to create volumes
  USERACL="$(maprcli acl show -noheader -type cluster -user $USER)"
  (( $? )) && err_exit "User $USER must have MapR login permission.  Modify with 'maprcli acl edit'"
  USERACL=${USERACL##*[}
  [[ $USERACL =~ cv ]]  || err_exit "User $USER must have MapR cv permission.  Modify with 'maprcli acl edit'"
}

check_prerequisites

# You can speed things up a bit by specifying the cluster name
# with the undocumented -C option.  I still check that it's valid.
if [[ -z $CLUSTERNAME ]]; then
  CMD="maprcli dashboard info -json"
  if (($VERBOSE)); then echo $CMD; fi
  CLUSTERNAME=$($CMD | grep name | cut -d'"' -f 4 | head -1)
fi

CMD="maprcli dashboard info -cluster $CLUSTERNAME"
if (($VERBOSE)); then echo $CMD; fi

if ! $CMD  > /dev/null 2>&1 ; then
  err_exit "Invalid cluster $CLUSTERNAME"
fi

if [[ -z $VOLNAMEBASE ]]; then
  err_exit "Volume name base must be specified with -N option"
fi

if [[ -z $VOLDIRBASE ]]; then
  VOLDIRBASE=$VOLNAMEBASE
fi

if [[ $VOLNAMEBASE != ${VOLNAMEBASE%[0-9]} ]]; then
  err_exit "Volume name base ($VOLNAMEBASE) must not end with numeric digit"
fi

create_base_mount() {
  if [[ -z $BASEDIR ]]; then 
    if [[ $USER == "root" ]] || [[ $USER == "mapr" ]]; then
      return 0
    else
      err_exit "Base directory must be specified with -b option"
    fi
  fi
  exec_cmd hadoop fs -test -s $BASEDIR
  RC=$?
  if [[ $RC -eq 0 ]]; then	# $BASEDIR exists
    exec_cmd hadoop fs -test -d $BASEDIR
    RC=$?
    if [[ $RC -ne 0 ]]; then	# $BASDIR is not a directory
      warn "$BASEDIR exists but it is not a directory"
      return 1
    fi
  else	# $BASEDIR does not exist
    exec_cmd hadoop fs -mkdir -p $BASEDIR
    exec_cmd hadoop fs -chmod 750 $BASEDIR
    exec_cmd hadoop fs -chown $ADMINUSER:$ADMINGROUP $BASEDIR
  fi
  return 0
}

read_volume_data() {
  unset SRCVOL
  # mirror type filter doesn't work.  see bug 19148
  #for VOL in $(maprcli volume list -filter [mirrorSrcVolume=="${VOLNAMEBASE}*"]and[mirrortype==2]  -columns mirrorSrcVolume -noheader)
  for VOL in $(maprcli volume list -filter [mirrorSrcVolume=="${VOLNAMEBASE}*"]and[volumetype==1] -columns mirrorSrcVolume -noheader)
  do
    if [[ ! $VOL =~ ${VOLNAMEBASE}[0-9]+ ]]; then
      # This VOL matches VOLNAMEBASE but not VOLNAMEBASE[0-9]+ so 
      # it must not belong to this mirror set
      continue
    fi
    # If SRCVOL not set, set it now
    # If already set but this source volume is different, it's an error
    if [[ -z $SRCVOL ]]; then
      SRCVOL=$VOL
    elif [[ ! $VOL == $SRCVOL ]]; then
      err_exit "Multiple source volumes (${VOL}, ${SRCVOL}) for mirror set."
    else
      continue
    fi
  done

  if [[ -z $SRCVOL ]]; then
    err_exit "No mirrors found for ${VOLNAMEBASE}"
  fi

  # We found a single source volume
  # Set it as the first volume in the list and append all of the mirrors to the array
  VOLARRAY[0]=$SRCVOL
  i=0
  for VOL in $(maprcli volume list -filter [mirrorSrcVolume=="${SRCVOL}"]  -columns volumename -noheader)
  do
    let i+=1
    VOLARRAY[$i]=$VOL
  done
  MIRRORS=$i

  # Set VOLDIRBASE and BASEDIR if not already set (eg create command not run in this invocation)
#set -x
  MOUNTPATH=$(maprcli volume list -noheader -filter "[volumename==$SRCVOL]" -columns mountdir | sed -e "s/ *$//" -e "s/[0-9]*$//")
  VOLDIRBASE=${MOUNTPATH##*/}
  BASEDIR=${MOUNTPATH%/*}
#set +x
}

list_volumes() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  if [[ $MIRRORS -gt -1 ]]; then
    echo Active: ${VOLARRAY[0]}
    for i in $(eval echo {1..$MIRRORS}) ; do
      echo Mirror: ${VOLARRAY[$i]}
    done
  fi
}

remove_volumes() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  # assert MIRRORS > -1
  echo "Completely remove the following volumes (not recoverable):"
  echo ${VOLARRAY[@]}
  echo -n "Continue [y/N]? "
  read -n 1 response
  echo ""
  if [[ ! $response =~ [yY] ]]; then
    echo "Operation aborted.  No volumes removed."
    return
  fi
  
  for i in $(eval echo {0..$MIRRORS}) ; do
    exec_cmd maprcli volume remove -force 1 -name ${VOLARRAY[$i]}
  done
  MIRRORS=-1
  unset VOLARRAY
}

create_volumes() {
  if [[ $MIRRORS -lt 1 ]]; then
    err_exit "Create action requires mirror count specification > 0 with -c option"
  fi
  exec_cmd create_base_mount
  if (($?));  then
    err_exit "Cannot create base directory $BASEDIR"
  fi

  #for i in $(eval echo {0..$MIRRORS}) ; do
  # Make sure VOLNAMEBASE is unique on the cluster
  VOLEXISTS=0
  #VOLEXISTS=$(maprcli volume list -filter "[volumename==${VOLNAMEBASE}*]"  -columns volumename -json | grep '"total":' | tr ',' ':' | cut -f2 -d':')
  for VOL in $( maprcli volume list -filter "[volumename==${VOLNAMEBASE}*]"  -columns volumename -noheader); do
    if [[ $VOL == ^${VOLNAMEBASE}[0-9]+ ]]; then
      let VOLEXISTS+=1
    fi
  done
  if (($VOLEXISTS)); then
    err_exit "$VOLEXISTS volume(s) with base name $VOLNAMEBASE exist(s)"
  fi

  # Create source volume.  This is initially ${VOLNAMEBASE}0 but this volume 
  # will become a mirror volume when one of the other mirror volumes is 
  # promoted to be the source volume.
  for i in $(eval echo {0..0}) ; do
    VOLARRAY[$i]=${VOLNAMEBASE}$i
    #MNTPATH=$BASEDIR/${VOLDIRBASE}$i
    MNTPATH=$BASEDIR/${VOLDIRBASE}
    exec_cmd maprcli volume create \
     -type rw \
     -aetype 0 \
     -quota $QUOTA \
     -name ${VOLNAMEBASE}$i \
     -path $MNTPATH \
     -user $ADMINUSER:dump,restore,m,d,a,fc \
     -rootdirperms $PERMS \
     -ae $ADMINUSER
    # -user $ADMINUSER:m,restore \
     #-user mapr:dump,restore,m,d,a,fc \

    exec_cmd hadoop fs -chown $ADMINUSER:$ADMINGROUP $MNTPATH
  done

  # Create mirror volumes
  for i in $(eval echo {1..$MIRRORS}) ; do
    VOLARRAY[$i]=${VOLNAMEBASE}$i
    MNTPATH=$BASEDIR/${VOLDIRBASE}$i
    # create RW volume set ownership.  Requires fc permission to start mirror.
    exec_cmd maprcli volume create \
     -type mirror \
     -aetype 0 \
     -quota $QUOTA \
     -source ${VOLARRAY[0]}@$CLUSTERNAME \
     -name ${VOLARRAY[$i]} \
     -path $MNTPATH \
     -user $ADMINUSER:dump,restore,m,d,a,fc \
     -rootdirperms $PERMS \
     -ae $ADMINUSER
     #-user mapr:dump,restore,m,d,a,fc \
  done
}

start_mirror() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  MIRRORNAME=$VOLNAMEBASE$MIRRORIDX
  if [[ $MIRRORNAME == ${VOLARRAY[0]} ]]; then
    warn "$MIRRORNAME is a RW volume, not a mirror.  No action taken."
    return
  fi
  exec_cmd maprcli volume mirror start -name $MIRRORNAME
}

# Check for mirror completion.  If done, remove snapshots.
check_mirror() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  MIRRORNAME=$VOLNAMEBASE$MIRRORIDX
  if [[ $MIRRORNAME == ${VOLARRAY[0]} ]]; then
    warn "$MIRRORNAME is a RW volume, not a mirror.  No action taken."
    return
  fi
  MPC=$(maprcli volume info -name $MIRRORNAME -columns mpc -noheader | sed -e 's/ //g')

  if [[ $MPC -eq 100 ]]; then
    echo "Mirror to $MIRRORNAME complete."
    # Create comma separated list of snapshots and delete them
    SNAPSHOTS=$(maprcli volume snapshot list -volume $MIRRORNAME -columns snapshotid -noheader | tr '\n' ',' | sed -e 's/ //g' -e 's/,$//' | cut -d"." -f1)
    if [[ ! -z $SNAPSHOTS ]]; then
      echo "Deleting mirror snapshots."
      exec_cmd maprcli volume snapshot remove -snapshots $SNAPSHOTS
    fi
  else
    echo "Mirror to $MIRRORNAME ${MPC}% complete."
    if $WAITCOMPLETE ; then sleep $SLEEPSECS; check_mirror; fi
  fi
}

unmount_volumes() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  # assert MIRRORS > -1
  for i in $(eval echo {0..$MIRRORS}) ; do
    exec_cmd maprcli volume unmount -name ${VOLARRAY[$i]}
  done
}

mount_volumes() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  # assert MIRRORS > -1
  
  # Mount first entry in VOLARRAY (source) under base dir name
  MNTPATH=$BASEDIR/${VOLDIRBASE}
  exec_cmd maprcli volume mount -name ${VOLARRAY[0]} -path $MNTPATH

  # Mount remaining entries (mirrors) with trailing digit corresponding to volume name
  for i in $(eval echo {1..$MIRRORS}) ; do
    #exec_cmd maprcli volume mount -name ${VOLARRAY[$i]}
    VOLNUM=${VOLARRAY[$i]#$VOLNAMEBASE}
    MNTPATH=$BASEDIR/${VOLDIRBASE}${VOLNUM}
    exec_cmd maprcli volume mount -name ${VOLARRAY[$i]} -path $MNTPATH
  done
}

promote_mirror() {
  if ((! ${#VOLARRAY[@]})); then
    read_volume_data
  fi
  # assert MIRRORS > -1
  #unmount_volumes
  MIRRORNAME=$VOLNAMEBASE$MIRRORIDX

  TMPARRAY[0]=$MIRRORNAME
  TMPPTR=1
  exec_cmd maprcli volume modify -type rw -name $MIRRORNAME || err_exit "unable to promote $MIRRORNAME"

  for i in $(eval echo {0..$MIRRORS}) ; do
    # Already promoted $MIRRORNAME so continue with next volume
    if [[ "$MIRRORNAME" == "${VOLARRAY[$i]}" ]]; then
      continue
    fi
    # First entry in VOLARRAY is always a standard volume, it needs to be changed to mirror.  Others are mirrors.
    TYPE=""; if [[ $i -eq 0 ]]; then TYPE="-type mirror"; fi
    # Just change source if volume is already a mirror.  If not, change type, too.
    exec_cmd maprcli volume modify $TYPE -source $MIRRORNAME@$CLUSTERNAME -name ${VOLARRAY[$i]} \
      || err_exit "unable to set mirror ${VOLARRAY[$i]} with source $MIRRORNAME@$CLUSTERNAME"
    TMPARRAY[$TMPPTR]=${VOLARRAY[$i]}
    let TMPPTR+=1
  done
  VOLARRAY=("${TMPARRAY[@]}")
}

do_actions() {
  for ACTION in $ACTIONS; do
    case $ACTION in
      "list" | "create" | "mount" | "unmount" | "remove")
        ${ACTION}_volumes
        ;;
      "sleep")
        [[ -z $SLEEPSECS ]] && SLEEPSECS=5
        sleep $SLEEPSECS
        ;;
      "start" | "check" | "promote")
        if [[ -z $MIRRORIDX ]]; then
	  err_exit "$ACTION requires mirror ID to be specified with -m option"
        fi
        ${ACTION}_mirror
        ;;
      *)
        #err_exit "Invalid action $ACTION"
        usage
        ;;
    esac
  done
}
do_actions

