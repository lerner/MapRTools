#!/bin/bash
# set -x

# Create a tmux session to view guts on a small cluster.
# 
# Create a tmux session "guts" with one pane per MapR node showing guts output for each node.  If multiple MFS instances on the node, shows
# guts summary info (eg /opt/mapr/bin/guts time:all db:none cpu:none).
#
# -w (instanceWindows)
# Create a tmux session "gutsw" with one tmux window per MFS instance (assumes same number of instances on each node in the cluster).  Each 
# window has one pane per MapR node showing guts output for a given MFS instance/port.  Cycle through windows/MFS ports with tmux ^b-n ^b-p.
# This invokes guts with the instance:$instanceId parameter
#
# -s (instanceSessions)
# Like -w option but each MFS instance/port is in a separate tmux session.  This allows simultaneous viewing of different ports across the
# cluster.  (tmux does not provide a mechanism to view different windows in the same session simultaneously)

# Assumes passwordless SSH to a MapR node for maprcli
# maprNode=mapr01

tmuxSessionBaseDefault=guts	# Default tmuxSessionBase
#tmuxSession=$tmuxSessionBase	# Default tmux session is simply base name

# MapR topology of nodes to run guts on.  Use this to avoid admin nodes or view one rack at a time if 
# too many nodes to view a pane in each node.  Set with -t option.
topologyDefault="/data/"; topology=$topologyDefault


# Tmux does not allow you to view different windows in the same session simultaneously.  
# Need to start a new session for each instance
instanceWindows=false
instanceSessions=false
maprNode=''
tmuxSessionBase=''

parseArgs() {
  while getopts "b:hm:st:w" OPTION
  do
    case $OPTION in 
      b) tmuxSessionBase=$OPTARG ;;
      h) errexit " " usage ;; # help
      m) # MapR node to execute maprcli and mrconfig commands on. Passwordless ssh assumed.
         # Default to first cldb node in /opt/mapr/conf/mapr-clusters.conf
         maprNode=$OPTARG ;;
      w) instanceWindows=true ;;
      s) instanceSessions=true ;;
      t) topology=$OPTARG ;; # Topology to run guts on.  Default to /data
      *) errexit "Unknown option $OPTION" usage ;;
    esac
  done
}

usage() {
  echo "Usage: $(basename $0) [-b tmux_session_basename] [-m mapr_node] [-t mapr_topology] [-s] [-w]"
}

errexit() {
  # Print message in $1 and execute command $2 then exit
  [[ ! -z $1 ]] && echo "$1"
  [[ ! -z $2 ]] && $2
  exit
}

parseArgs $@

setMaprNode() {
  if [[ -f /opt/mapr/conf/mapr-clusters.conf ]]; then
    maprNode=$(head -1 /opt/mapr/conf/mapr-clusters.conf  | tr -s ' ' | cut -f 3 -d ' ' | cut -f 1 -d ':')
  else
    errexit "No MapR node specified (-m) and no mapr-clusters.conf file to determine a MapR node." usage
  fi

  [[ -z $maprNode ]] && errexit "Could not determine MapR node from mapr-clusters.conf file." usage
}

validateNode() {
  # Confirm we can ssh to the node and execute a command
  
  # Set node if it isn't already set
  node=$1
  [[ -z $node ]] && setMaprNode
  node=$maprNode

  # Confirm we can ssh to the node
  ! ssh $node hostname > /dev/null && errexit "Could not passwordless ssh to MapR node $node"

}

[[ -z $tmuxSessionBase ]] && tmuxSessionBase=$tmuxSessionBaseDefault
$instanceWindows && tmuxSession=${tmuxSessionBase}w

# [[ -z maprNode ]] && setMaprNode
validateNode $maprNode

nodeJson=$(ssh $maprNode maprcli node list -json)
[[ -z $nodeJson ]] && errexit "Unable to determine cluster node information."

if [[ $(echo $nodeJson | jq -r .status) = "ERROR" ]]; then
  errexit "maprcli failure from $maprNode: $(echo $nodeJson | jq -r .errors[].desc)"
fi

jqCmd=".data[] | select(.racktopo | startswith(\"$topology\")) | .hostname"
declare -a nodeArr=( $(echo $nodeJson | jq -r "$jqCmd" ) )
nodeCnt=${#nodeArr[@]}

declare portArr=()
if $instanceWindows || $instanceSessions; then
  portArr=( $(ssh ${nodeArr[0]} /opt/mapr/server/mrconfig info instances | tail -1) )
  [[ -z portArr ]] && errexit "Unable to determine mfs instance ports with \"ssh ${nodeArr[0]} /opt/mapr/server/mrconfig info instances\""
  # If instance sessions (-s), first session will be base session name with port number appended
  $instanceSessions && tmuxSession=${tmuxSessionBase}${portArr[0]}  # First of tmux sessions.  There will be one per port.
else
  portArr=( 5660 )
  tmuxSession=${tmuxSessionBase}  # No -w or -s option specified.  One tmux session with all guts instances combined.
fi

# If the tmux session already exists, print attach info and exit
tmux has-session -t $tmuxSession && errexit "tmux session '$tmuxSession' already running.  Run 'tmux attach -t $tmuxSession' to attach."

initialWindow=${tmuxSession}.summary
$instanceWindows && initialWindow=${tmuxSession}.${portArr[0]}
$instanceSessions && initialWindow=${tmuxSessionBase}${portArr[0]}

tmuxCmd="tmux new-session -d -n $initialWindow -s $tmuxSession -x 144"
echo $tmuxCmd
$tmuxCmd
currentWindow=$initialWindow

  for port in ${portArr[@]} ; do
    # If not first port, then we have -s or -w.  Create new session or new window.
    if [[ $port -ne ${portArr[0]} ]]; then
      $instanceWindows && tmux new-window -n ${tmuxSession}.$port 
      $instanceSessions && tmux new-session -d -n ${tmuxSessionBase}.$port -s ${tmuxSessionBase}$port -x 144
      currentWindow=${tmuxSession}.$port
    fi

    #  For each node in the topology, split the current window.  We will end up with one extra window on top for headers
    for i in $(eval echo {1..$nodeCnt}); do
      tmux split-window -h
      #tmux select-layout tiled
      tmux select-layout even-vertical
    done

    # Print the guts header in the first window 0
    # ssh into each of the cluster nodes in windows 1..nodeCnt
    i=0
    while [[ $i -le $nodeCnt ]]; do
      if [[ $i -eq 0 ]] ; then
        tmux send-keys -t $i "ssh ${nodeArr[0]}" Enter 
        sleep 1
        tmux send-keys -t $i "set -o vi" Enter
        tmux send-keys -t $i "/opt/mapr/bin/guts time:all db:none cpu:none header:none flush:line > /tmp/guts.header" Enter
        sleep 1
        tmux send-keys -t $i C-c "grep rpc /tmp/guts.header" Enter
        let i+=1
      fi
      # In window 1, ssh to nodeArr[0]; window N, ssh to nodeArr[N-1]
      let j=$i-1
      tmux send-keys -t $i "ssh ${nodeArr[$j]}" Enter 
      let i+=1
    done
    sleep 1 # To make sure ssh completes
    i=1

    # Invoke guts on each node
    while [[ $i -le $nodeCnt ]]; do
      tmux send-keys -t $i "set -o vi" Enter
      let instanceId=${port}-5660
      instanceArg="instance:$instanceId"
      ! $instanceSessions && ! $instanceWindows && instanceArg=""
      tmux send-keys -t $i "/opt/mapr/bin/guts time:all db:none cpu:none header:none $instanceArg" Enter
      let i+=1
    done
    tmux set-option -t $currentWindow synchronize-panes on   
  done
  if $instanceSessions; then
    echo "execute 'tmux attach -t ${tmuxSessionBase}<port>' where port is one of [ ${portArr[@]} ] to connect:"
    for port in ${portArr[@]}; do
      echo "         tmux attach -t ${tmuxSessionBase}$port"
    done
  else
    echo "execute 'tmux attach -t $tmuxSession' to connect"
  fi

