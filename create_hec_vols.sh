#!/bin/bash  

# Author: Andy Lerner
# Update History:
#  03Nov2017    Removed need for client numbering.  Use clush groups to determine clients

# This script can be run from any client node with clush access to all clients and cluster nodes
# ssh to cluster node now used - no dependence on maprcli on client nodes

HANA=hana # Volume and directory names base

################################################################################
# SET VARIABLE VALUES IN THIS SECTION
################################################################################
# 1. Set CLUSTER to the cluster name of a cluster and CLUSTERNODE to any MapR node on the cluster
#    $CLUSTER MUST be the cluster name in /opt/mapr/conf/mapr-clusters.conf AND the clush group name
#    for all cluster nodes.
CLUSTERNAME=hec2_cluster; CLUSTERNODE=lnvmaprc1n01

#CLUSTER=scale62 ; CLUSTERNODE=scale-64

# 2. Set CLUSH_CLIENTS to the clush group name of the client nodes
CLUSH_CLIENTS_GRP=hana
#CLUSH_CLIENTS_GRP=hana_x16
CLUSH_DATANODES_GRP=hec2_cluster # Just data nodes (not including control nodes)
#CLUSH_DATANODES_GRP=hec2_cluster_x8 # Just data nodes (not including control nodes)
CLUSH_CLUSTER_GRP=hec2_cluster # Entire cluster (including control nodes)

# STEP 3 No longer necessary
# 3. Set client numbering.  Hana volumes will be created for each client with client number
#STARTCLIENT=1; ENDCLIENT=16
#STARTCLIENT=7; ENDCLIENT=18
#STARTCLIENT=67; ENDCLIENT=67

# 4. Set the desired number of master containers for calculating data to be dd'd
#MASTER_CNTRS_PER_SP=3 # precreate_containers() master containers per storage pool per volume 
MASTER_CNTRS_PER_SP=3

# 5. Set MapR replication for volume creation
REPL=3
MINREPL=2

# 6. a) Set a small container size for pre-creation of containers
#    b) Once container size has been set, comment out following 2 lines and re-run script

##### Once container size has been set, comment out these 2 lines and run script
#ssh $CLUSTERNODE maprcli config save -values '{"cldb.container.sizemb":"1024"}'
#clush -B -w @$CLUSH_CLUSTER_GRP systemctl restart  mapr-warden ; echo "After cluster restart, comment out section 6 in script and re-run"; exit

####  NO LONGER NECESSARY.  NOT RUNNING SSH PER CLIENT ####
# 7. IF TOTAL CLIENTS IS >10, update /etc/ssh/sshd_config for 2x total clients and 
#    restart sshd on CLUSTERNODE (systemctl restart sshd).

# If there are 20 clients, set MaxSessions and first var of MaxStartups to 40.

## grep MaxS /etc/ssh/sshd_config
##MaxSessions 10
#MaxSessions 40
##MaxStartups 10:30:100
#MaxStartups 40:30:100

################################################################################

# Create space separated list of client hostname numbers without leading zeros
shopt -s extglob
CLIENT_NUMS=( $(for NODE in $(nodeset -e @$CLUSH_CLIENTS_GRP); do NUM=${NODE##${NODE%%+([0-9])}}; echo -n "${NUM##*(0)} " ;  done) )

# Set up .cluster_env to be sourced for environment variables on subsequent clush calls

#let NUM_CLIENTS=ENDCLIENT-STARTCLIENT+1
let NUM_CLIENTS=${#CLIENT_NUMS[@]}
rm -f /tmp/.cluster_env
echo "export CLUSTERNAME=$CLUSTERNAME" >> /tmp/.cluster_env
echo "export CLIENT_NUMS=( ${CLIENT_NUMS[@]} )" >> /tmp/.cluster_env
echo "export STARTCLIENT=$STARTCLIENT" >> /tmp/.cluster_env
echo "export ENDCLIENT=$ENDCLIENT" >> /tmp/.cluster_env
echo "export NUM_CLIENTS=$NUM_CLIENTS" >> /tmp/.cluster_env
echo "export HANA=$HANA" >> /tmp/.cluster_env
clush -c -w @$CLUSH_CLIENTS_GRP /tmp/.cluster_env
clush -c -w $CLUSTERNODE /tmp/.cluster_env


remove_vols_per_client_old() {
  echo $FUNCNAME
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTERNAME -name ${HANA}.log.n$i &
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTERNAME -name ${HANA}.data.n$i &
  done
  wait
  echo ""
}

remove_vols_per_client() {
  echo $FUNCNAME
  for V in log data ; do
    printf "%5s: " $V
    ssh $CLUSTERNODE "for i in ${CLIENT_NUMS[@]}; do echo -n \"\$i \"; maprcli volume remove -cluster $CLUSTERNAME -name ${HANA}.${V}.n\$i & done; wait"
    echo ""
  done
}

remove_top_level_volumes() {
  echo $FUNCNAME
  
  for V in log data shared backup; do
  #for V in data ; do
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTERNAME -name ${HANA}.$V 
  done
  ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTERNAME -name ${HANA} 
}

create_top_level_volumes() {
  echo $FUNCNAME
  ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTERNAME -name ${HANA} -path /apps/${HANA} -replication $REPL -minreplication $MINREPL

  ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTERNAME -name ${HANA}.log -path /apps/${HANA}/log -replicationtype low_latency  -replication $REPL -minreplication $MINREPL
  for V in data shared backup; do
  #for V in data ; do
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTERNAME -name ${HANA}.$V -path /apps/${HANA}/$V -replicationtype high_throughput  -replication $REPL -minreplication $MINREPL
  done
  
#  for V in log data; do
  for V in log data; do
    hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/$V
  done

  for V in " " /log /data /shared /backup; do
    hadoop mfs -setnetworkencryption on /mapr/$CLUSTERNAME/apps/${HANA}$V
  done

}

create_vols_per_client() {
  echo $FUNCNAME
  for V in log data ; do
    [[ $V = "log" ]] && REPTYPE=low_latency
    [[ $V = "data" ]] && REPTYPE=high_throughput
    printf "%5s: " $V
    ssh $CLUSTERNODE "for i in ${CLIENT_NUMS[@]}; do echo -n \"\$i \"; maprcli volume create -cluster $CLUSTERNAME -name ${HANA}.${V}.n\$i -path /apps/${HANA}/$V/n\$i -replicationtype $REPTYPE  -replication $REPL -minreplication $MINREPL & done; wait"
    echo ""
  done
  echo "set compression off and encryption on "
  for V in log data ; do
    printf "%5s: " $V
    for i in ${CLIENT_NUMS[@]} ; do 
      echo -n "$i "
      hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/$V/n$i  &
      hadoop mfs -setnetworkencryption on /mapr/$CLUSTERNAME/apps/${HANA}/$V/n$i  &
    done
    wait
    echo " "
  done
}

create_vols_per_client_old() {
  echo $FUNCNAME
  echo -n "create volume "
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTERNAME -name ${HANA}.log.n$i -path /apps/${HANA}/log/n$i -replicationtype low_latency  -replication $REPL -minreplication $MINREPL &
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTERNAME -name ${HANA}.data.n$i -path /apps/${HANA}/data/n$i -replicationtype high_throughput  -replication $REPL -minreplication $MINREPL &
  done
  wait
  echo ""
  echo -n "set compression off and encryption on "
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/log/n$i  &
    hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/data/n$i &
    hadoop mfs -setnetworkencryption on /mapr/$CLUSTERNAME/apps/${HANA}/log/n$i  &
    hadoop mfs -setnetworkencryption on /mapr/$CLUSTERNAME/apps/${HANA}/data/n$i &
  done
  wait
  echo ""
}

create_piodir_per_chunkdir() {
  echo $FUNCNAME
  #for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
  for i in ${CLIENT_NUMS[@]} ; do 
    echo -n "$i "
    for CHUNKMB in 2 4 8 16 32 64 256; do
      for V in data log ; do
        NEWDIR="/mapr/$CLUSTERNAME/apps/${HANA}/$V/n$i/chunk${CHUNKMB}MB"
        mkdir $NEWDIR/pio &
      done
    done
  done
  wait
  echo ""
}
create_chunkdirs_per_vol() {
  echo $FUNCNAME
  #for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
  for i in ${CLIENT_NUMS[@]} ; do 
    echo $i
    for CHUNKMB in 2 4 8 16 32 64 256; do
      for V in data log ; do
        NEWDIR="/mapr/$CLUSTERNAME/apps/${HANA}/$V/n$i/chunk${CHUNKMB}MB"
        mkdir $NEWDIR 
        let CHUNKBYTES=$CHUNKMB*1024*1024
        echo hadoop mfs -setchunksize $CHUNKBYTES $NEWDIR
        hadoop mfs -setchunksize $CHUNKBYTES $NEWDIR &
      done
    done
  done
  wait
  create_piodir_per_chunkdir
}

# Create a link so the same path on each client from /home/mapr goes to a separate volume
# Assume last two characters of client hostname are numeric host number
create_link_per_client() {
  echo $FUNCNAME
  : 
  clush -B -w @$CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
		     H=$(hostname -s); \
		     H=${H: -2}; H=${H#0}; \
                     for V in data log; do \
                       [[ ! -d /home/mapr/$CLUSTERNAME/apps/${HANA} ]] && mkdir -p /home/mapr/$CLUSTERNAME/apps/${HANA}; \
                       rm -f /home/mapr/$CLUSTERNAME/apps/${HANA}/$V; \
                       ln -s /mapr/$CLUSTERNAME/apps/${HANA}/$V/n$H /home/mapr/$CLUSTERNAME/apps/${HANA}/$V; \
                     done'
}

precreate_containers() {
  echo $FUNCNAME
  # MASTER_CNTRS_PER_SP=3 # Number of master containers per storage pool per volume
  NUMSPS=$(ssh $CLUSTERNODE /opt/mapr/server/mrconfig sp list  | grep path | wc -l)
  MB_PER_CNTR=$(ssh $CLUSTERNODE maprcli config load -noheader -keys cldb.container.sizemb)
  # Use clush group rather than servers running fileserver in case cldb nodes are NOT data nodes 
  #NUM_NODES=$(ssh $CLUSTERNODE 'maprcli node list -filter "[svc==fileserver]" -columns hostname -noheader | wc -l' )
  NUM_NODES=$(nodeset -c @$CLUSH_DATANODES_GRP)
  #CNTRS_PER_VOL=$(echo $NUMSPS*$MASTER_CNTRS_PER_SP | bc)
  #CNTRS_PER_VOL=$(echo $NUMSPS*$MASTER_CNTRS_PER_SP*$NUM_NODES | bc)
  let CNTRS_PER_VOL=NUMSPS*MASTER_CNTRS_PER_SP*NUM_NODES 
  #MB_PER_VOL=$(echo $MB_PER_CNTR*$CNTRS_PER_VOL | bc)
  let MB_PER_VOL=MB_PER_CNTR*CNTRS_PER_VOL
  NUM_DDS=10
  #MB_PER_DD=$(echo $MB_PER_VOL/$NUM_DDS | bc)
  let MB_PER_DD=$MB_PER_VOL/$NUM_DDS
  echo "export NUM_DDS=$NUM_DDS" >> /tmp/.cluster_env
  echo "export MB_PER_DD=$MB_PER_DD" >> /tmp/.cluster_env
  clush -c -w @$CLUSH_CLIENTS_GRP /tmp/.cluster_env
echo MASTER_CNTRS_PER_SP=$MASTER_CNTRS_PER_SP
echo NUMSPS=$NUMSPS
echo MB_PER_CNTR=$MB_PER_CNTR
echo NUM_NODES=$NUM_NODES
echo CNTRS_PER_VOL=$CNTRS_PER_VOL
echo MB_PER_VOL=$MB_PER_VOL
echo NUM_DDS=$NUM_DDS
echo MB_PER_DD=$MB_PER_DD

echo Total Root Data   = $MB_PER_VOL MB
echo Total Root Log    = $MB_PER_VOL MB
echo Total Root Backup = $MB_PER_VOL MB
echo Total Client Data = $(let x=$MB_PER_VOL*$NUM_CLIENTS ; echo $x) MB
echo Total Client Log  = $(let x=$MB_PER_VOL*$NUM_CLIENTS ; echo $x) MB

  # Create dd directories
                       #hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/$V/n$H/dd ;
                       hadoop mfs -setcompression off /mapr/$CLUSTERNAME/apps/${HANA}/backup ;
                       hadoop mfs -setnetworkencryption on /mapr/$CLUSTERNAME/apps/${HANA}/backup ;
  clush -B -w @$CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
		     H=$(hostname -s); \
		     H=${H: -2}; H=${H#0}; \
                     let B_MB_PER_DD=$MB_PER_DD/$NUM_CLIENTS ; \
		     for V in backup data log ; do \
		       for i in $(eval echo {1..$NUM_DDS}) ; do \
		         dd if=/dev/zero of=/mapr/$CLUSTERNAME/apps/${HANA}/$V/dd.${B_MB_PER_DD}MB.node$H.$i bs=1024k count=$B_MB_PER_DD & 
		       done; \
		     done; \
		     wait; \
		     for V in backup data log ; do \
		       rm -rf /mapr/$CLUSTERNAME/apps/${HANA}/$V/dd*node$H.* & 
		     done; \
		     wait ; \
		     for V in data log; do \
		       mkdir /mapr/$CLUSTERNAME/apps/${HANA}/$V/n${H}/dd; \
		       for i in $(eval echo {1..$NUM_DDS}) ; do \
		         dd if=/dev/zero of=/home/mapr/$CLUSTERNAME/apps/${HANA}/$V/dd/dd.${MB_PER_DD}MB.$i bs=1024k count=$MB_PER_DD & 
		       done; \
		       wait; \
		       rm -rf /home/mapr/$CLUSTERNAME/apps/${HANA}/$V/dd/* ; \
		     done; \
		     '
                       hadoop mfs -setcompression on /mapr/$CLUSTERNAME/apps/${HANA}/backup ;
}

remove_dd_files() {
  clush -B -w @$CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
	for V in data log; do \
	  rm -rf /home/mapr/$CLUSTERNAME/apps/${HANA}/$V/dd/* ; \
	done; \
	'
}

remove_pio_files() {
  echo $FUNCNAME
  clush -B -w @$CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
                     for CHUNKMB in 2 4 8 16 32 64 256; do \
                       for V in data log ; do \
                         rm -f /home/mapr/$CLUSTERNAME/apps/${HANA}/$V/chunk${CHUNKMB}MB/pio/* ; \
		       done; \
		     done; \
		     '
}


# get container count for each volume
get_container_count_old() {  
  echo $FUNCNAME
    ssh $CLUSTERNODE '. /tmp/.cluster_env; for V in log data; do for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do echo -n "${HANA}.${V}.n$i: "; /opt/mapr/server/mrconfig info containerlist ${HANA}.$V.n$i | wc -l; done; done'
}

get_container_count() {  
  echo $FUNCNAME
    echo $CLUSTERNODE
    ssh $CLUSTERNODE '. /tmp/.cluster_env; for V in log data; do for i in ${CLIENT_NUMS[@]} ; do echo -n "${HANA}.${V}.n$i: "; /opt/mapr/server/mrconfig info containerlist ${HANA}.$V.n$i | wc -l; done; done'
}

remove_vols_per_client
remove_top_level_volumes
create_top_level_volumes
create_vols_per_client
create_chunkdirs_per_vol
create_link_per_client
precreate_containers
remove_dd_files
remove_pio_files
get_container_count

echo "Current container size: $(ssh $CLUSTERNODE 'maprcli config load -json | grep container.sizemb')"
echo "Run these commands to reset to default value 32768:"
printf "%s%s\n" "ssh $CLUSTERNODE maprcli config save -values {" '"cldb.container.sizemb":"32768"}'
echo "clush -B -w@$CLUSH_CLUSTER_GRP service mapr-warden restart"
