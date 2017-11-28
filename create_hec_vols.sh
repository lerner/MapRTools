#!/bin/bash  

# This script can be run from any client node with clush access to all clients and cluster nodes
# ssh to cluster node now used - no dependence on maprcli on client nodes

################################################################################
# SET VARIABLE VALUES IN THIS SECTION
################################################################################
# 1. Set CLUSTER to the cluster name of a cluster and CLUSTERNODE to any MapR node on the cluster
#    $CLUSTER MUST be the cluster name in /opt/mapr/conf/mapr-clusters.conf AND the clush group name
#    for all cluster nodes.
CLUSTER=sdisk ; CLUSTERNODE=b140-sdisk01
#CLUSTER=scale62 ; CLUSTERNODE=scale-64

# 2. Set CLUSH_CLIENTS to the clush group name of the client nodes
CLUSH_CLIENTS_GRP=clients2
#CLUSH_CLIENTS_GRP=clients7

# 3. Set client numbering.  Hana volumes will be created for each client with client number
STARTCLIENT=1; ENDCLIENT=20
#STARTCLIENT=7; ENDCLIENT=18
#STARTCLIENT=67; ENDCLIENT=67

# 4. Set the desired number of master containers for calculating data to be dd'd
#MASTER_CNTRS_PER_SP=3 # precreate_containers() master containers per storage pool per volume 
MASTER_CNTRS_PER_SP=3

# 5. a) Set a small container size for pre-creation of containers
#    b) Once container size has been set, comment out following 2 lines and re-run script

##### Once container size has been set, comment out these 2 lines and run script
ssh $CLUSTERNODE maprcli config save -values '{"cldb.container.sizemb":"2048"}'
clush -Bg $CLUSTER service mapr-warden restart; echo "After cluster restart, comment out section 5 in script and re-run"; exit


# 6. IF TOTAL CLIENTS IS >10, update /etc/ssh/sshd_config for 2x total clients and 
#    restart sshd on CLUSTERNODE (systemctl restart sshd).

# If there are 20 clients, set MaxSessions and first var of MaxStartups to 40.

## grep MaxS /etc/ssh/sshd_config
##MaxSessions 10
#MaxSessions 40
##MaxStartups 10:30:100
#MaxStartups 40:30:100

# 7. Set MapR replication for volume creation
REPL=2
MINREPL=2

################################################################################

# Set up .cluster_env to be sourced for environment variables on subsequent clush calls
let NUM_CLIENTS=ENDCLIENT-STARTCLIENT+1
echo "export CLUSTER=$CLUSTER" > /tmp/.cluster_env
echo "export STARTCLIENT=$STARTCLIENT" >> /tmp/.cluster_env
echo "export ENDCLIENT=$ENDCLIENT" >> /tmp/.cluster_env
echo "export NUM_CLIENTS=$NUM_CLIENTS" >> /tmp/.cluster_env
clush -cg $CLUSH_CLIENTS_GRP /tmp/.cluster_env
clush -cw $CLUSTERNODE /tmp/.cluster_env


remove_vols_per_client() {
  echo $FUNCNAME
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTER -name hana.log.n$i &
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTER -name hana.data.n$i &
  done
  wait
  echo ""
}

remove_top_level_volumes() {
  echo $FUNCNAME
  ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTER -name hana.log 
  
  for V in data shared backup; do
  #for V in data ; do
    ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTER -name hana.$V 
  done
  ssh $CLUSTERNODE maprcli volume remove -cluster $CLUSTER -name hana 
}

create_top_level_volumes() {
  echo $FUNCNAME
  ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTER -name hana -path /apps/hana -replication $REPL -minreplication $MINREPL
  ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTER -name hana.log -path /apps/hana/log -replicationtype low_latency  -replication $REPL -minreplication $MINREPL
  
  for V in data shared backup; do
  #for V in data ; do
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTER -name hana.$V -path /apps/hana/$V -replicationtype high_throughput  -replication $REPL -minreplication $MINREPL
  done
  
#  for V in log data; do
  for V in log data; do
    hadoop mfs -setcompression off /mapr/$CLUSTER/apps/hana/$V
  done
}

create_vols_per_client() {
  echo $FUNCNAME
  echo -n "create volume "
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTER -name hana.log.n$i -path /apps/hana/log/n$i -replicationtype low_latency  -replication $REPL -minreplication $MINREPL &
    ssh $CLUSTERNODE maprcli volume create -cluster $CLUSTER -name hana.data.n$i -path /apps/hana/data/n$i -replicationtype high_throughput  -replication $REPL -minreplication $MINREPL &
  done
  wait
  echo ""
  echo -n "set compression off "
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    hadoop mfs -setcompression off /mapr/$CLUSTER/apps/hana/log/n$i  &
    hadoop mfs -setcompression off /mapr/$CLUSTER/apps/hana/data/n$i &
  done
  wait
  echo ""
}

create_piodir_per_chunkdir() {
  echo $FUNCNAME
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo -n "$i "
    for CHUNKMB in 2 4 8 16 32 64 256; do
      for V in data log ; do
        NEWDIR="/mapr/$CLUSTER/apps/hana/$V/n$i/chunk${CHUNKMB}MB"
        mkdir $NEWDIR/pio &
      done
    done
  done
  wait
  echo ""
}
create_chunkdirs_per_vol() {
  echo $FUNCNAME
  for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do 
    echo $i
    for CHUNKMB in 2 4 8 16 32 64 256; do
      for V in data log ; do
        NEWDIR="/mapr/$CLUSTER/apps/hana/$V/n$i/chunk${CHUNKMB}MB"
        mkdir $NEWDIR 
        CHUNKBYTES=$(echo $CHUNKMB*1024*1024 | bc)
        echo hadoop mfs -setchunksize $CHUNKBYTES $NEWDIR
        hadoop mfs -setchunksize $CHUNKBYTES $NEWDIR &
      done
    done
  done
  wait
  create_piodir_per_chunkdir
}

# Create a link so the same path on each client from /home/mapr goes to a separate volume
create_link_per_client() {
  echo $FUNCNAME
  : 
  clush -Bg $CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
                     H=$(hostname -s | cut -f 2 -d "-" | cut -f1 -d"p"); \
                     H=${H#0}; \
                     for V in data log; do \
                       [[ ! -d /home/mapr/$CLUSTER/apps/hana ]] && mkdir -p /home/mapr/$CLUSTER/apps/hana; \
                       rm -f /home/mapr/$CLUSTER/apps/hana/$V; \
                       ln -s /mapr/$CLUSTER/apps/hana/$V/n$H /home/mapr/$CLUSTER/apps/hana/$V; \
                     done'
}

precreate_containers() {
  echo $FUNCNAME
  # MASTER_CNTRS_PER_SP=3 # Number of master containers per storage pool per volume
  NUMSPS=$(ssh $CLUSTERNODE /opt/mapr/server/mrconfig sp list  | grep path | wc -l)
  MB_PER_CNTR=$(ssh $CLUSTERNODE maprcli config load -noheader -keys cldb.container.sizemb)
  NUM_NODES=$(ssh $CLUSTERNODE 'maprcli node list -filter "[svc==fileserver]" -columns hostname -noheader | wc -l' )
  #CNTRS_PER_VOL=$(echo $NUMSPS*$MASTER_CNTRS_PER_SP | bc)
  CNTRS_PER_VOL=$(echo $NUMSPS*$MASTER_CNTRS_PER_SP*$NUM_NODES | bc)
  MB_PER_VOL=$(echo $MB_PER_CNTR*$CNTRS_PER_VOL | bc)
  NUM_DDS=10
  MB_PER_DD=$(echo $MB_PER_VOL/$NUM_DDS | bc)
  echo "export NUM_DDS=$NUM_DDS" >> /tmp/.cluster_env
  echo "export MB_PER_DD=$MB_PER_DD" >> /tmp/.cluster_env
  clush -cg $CLUSH_CLIENTS_GRP /tmp/.cluster_env
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
echo Total Client Data = $(echo $MB_PER_VOL*$NUM_CLIENTS | bc) MB
echo Total Client Log  = $(echo $MB_PER_VOL*$NUM_CLIENTS | bc) MB

  # Create dd directories
                       #hadoop mfs -setcompression off /mapr/$CLUSTER/apps/hana/$V/n$H/dd ;
                       hadoop mfs -setcompression off /mapr/$CLUSTER/apps/hana/backup ;
  clush -Bg $CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
                     H=$(hostname -s | cut -f 2 -d "-" | cut -f1 -d"p"); \
                     H=${H#0}; \
                     B_MB_PER_DD=$(echo $MB_PER_DD/$NUM_CLIENTS | bc) ; \
		     for V in backup data log ; do \
		       for i in $(eval echo {1..$NUM_DDS}) ; do \
		         dd if=/dev/zero of=/mapr/$CLUSTER/apps/hana/$V/dd.${B_MB_PER_DD}MB.node$H.$i bs=1024k count=$B_MB_PER_DD & 
		       done; \
		     done; \
		     wait; \
		     for V in backup data log ; do \
		       rm -rf /mapr/$CLUSTER/apps/hana/$V/dd*node$H.* & 
		     done; \
		     wait ; \
		     for V in data log; do \
		       mkdir /mapr/$CLUSTER/apps/hana/$V/n${H}/dd; \
		       for i in $(eval echo {1..$NUM_DDS}) ; do \
		         dd if=/dev/zero of=/home/mapr/$CLUSTER/apps/hana/$V/dd/dd.${MB_PER_DD}MB.$i bs=1024k count=$MB_PER_DD & 
		       done; \
		       wait; \
		       rm -rf /home/mapr/$CLUSTER/apps/hana/$V/dd/* ; \
		     done; \
		     '
                       hadoop mfs -setcompression on /mapr/$CLUSTER/apps/hana/backup ;
}

remove_dd_files() {
  clush -Bg $CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
	for V in data log; do \
	  rm -rf /home/mapr/$CLUSTER/apps/hana/$V/dd/* ; \
	done; \
	'
}

remove_pio_files() {
  echo $FUNCNAME
  clush -Bg $CLUSH_CLIENTS_GRP '. /tmp/.cluster_env; \
                     for CHUNKMB in 2 4 8 16 32 64 256; do \
                       for V in data log ; do \
                         rm -f /home/mapr/$CLUSTER/apps/hana/$V/chunk${CHUNKMB}MB/pio/* ; \
		       done; \
		     done; \
		     '
}


# get container count for each volume
get_container_count() {  
  echo $FUNCNAME
    ssh $CLUSTERNODE '. /tmp/.cluster_env; for V in log data; do for i in $(eval echo {$STARTCLIENT..$ENDCLIENT}) ; do echo -n "hana.${V}.n$i: "; /opt/mapr/server/mrconfig info containerlist hana.$V.n$i | wc -l; done; done'
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
echo "clush -Bg $CLUSTER service mapr-warden restart"
