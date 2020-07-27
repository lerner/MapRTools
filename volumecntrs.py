#!/usr/bin/python
#
# Show distribution of MapR volumes' containers across MFS instances
#
# Requirements:
#   mapr-core is installed for maprcli command
#
# Usage: volumemcntrs.py <comma separated list of name of volume names>
# Author: Andy Lerner
# Examples:
#   Containers for just the users volume:
#     volumecntrs.py users
#
#   Containers for volumes users and mapr.var:
#     volumecntrs.py users,mapr.var
#
#   Containers for all volumes in the cluster:
#     volumecntrs.py $(maprcli volume list -noheader -columns volumename | tr '\n' ',' | sed -e 's/ //'g -e 's/,$//')
#
# Change history:
#   2017 Sep 29	Added Totals to heatmap.  
#               Modified for 6.0 JSON ActiveServers key IP:Port changed to IP
#   2017 Oct 11 Resolved IP output into hostname and sort output on hostname
#               Check MapR Version and set ActiveServers key to IP:Port or IP as appropriate
#   2017 Oct 16 Fix for hostnames greater than 16 characters
#   2020 Jan 16 Handle multiple networks, Append ecstore container info if any EC volumes specified.


import sys
import subprocess
import re
import json
import locale
import socket
from collections import defaultdict
from copy import deepcopy

locale.setlocale(locale.LC_ALL, 'en_US')
progname=sys.argv[0]

def usage(*ustring):
  print 'Usage: '+ progname + ' <Comma separated list of volume names>'
  if len(ustring) > 0:
    print "       ",ustring[0]
  exit(1)

def errexit(estring, err):
  print estring,": ",err
  exit(1)

def execCmd(cmdlist):
  process = subprocess.Popen(cmdlist, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, err = process.communicate()
  return out,err

def getClusterInfo():
  clusterInfo, errout = execCmd(["/opt/mapr/bin/maprcli", "dashboard", "info", "-json"])
  clusterInfoJson=json.loads(clusterInfo)
  return clusterInfoJson

def getNodeInfo():
  nodeInfo, errout = execCmd(["/opt/mapr/bin/maprcli", "node", "list", "-filter", "[svc==fileserver]", "-json"])
  nodeInfoJson=json.loads(nodeInfo)
  return nodeInfoJson

# This should have been getVolumeDumpInfo, but leaving it
def getVolumeInfo(volumename):
  volumeInfo, errout = execCmd(["/opt/mapr/bin/maprcli", "dump", "volumeinfo", "-volumename", volumename, "-json"])
  volumeInfoJson=json.loads(volumeInfo)
  return volumeInfoJson

# Adding getVolumeInfo2, for what should be getVolumeInfo, for erasure coding info
def getVolumeInfo2(volumename):
  volumeInfo, errout = execCmd(["/opt/mapr/bin/maprcli", "volume", "info", "-name", volumename, "-json"])
  volumeInfo2Json=json.loads(volumeInfo)
  return volumeInfo2Json

def isValidInfoJson(InfoJson):
  # region info JSON should always have a status
  desc = "NOT_OK"
  status = "NOT_OK"
  if "status" in InfoJson:
    desc = status = InfoJson["status"]
  if "errors" in InfoJson:
    desc = InfoJson["errors"][0]["desc"]
  return status, desc

def container_fmt(container, isMaster=False, isValid=True, isEcStore=False):
  rs=""
  if not isEcStore:
    if isMaster:
      rs+="'"
    if not isValid:
      rs+="*"
  rs+=str(container)
  return rs

def print_containers():
  instanceContainerList=[]
  for instance in instanceList:
    ip=instance[:instance.find(':')]
    port=instance[instance.find(':')+1:]
    host=socket.gethostbyaddr(ip)[0]
    instanceContainerLine = host + ':' + port + " :: "
    # print instance + " ::",
    # removed later indented comments below
    for container in volumeContainers[instance]:
      #print container,
      instanceContainerLine+= container + " "
    #print ""
    instanceContainerList.append(instanceContainerLine)
  instanceContainerList.sort()
  print "\n".join(instanceContainerList)

def print_containers_ip():
  for instance in instanceList:
    print instance + " ::",
    # removed later indented comments below
    for container in volumeContainers[instance]:
      print container,
    print ""

def print_heatmap(containerType):
    # Assume all hostnames are the same length and set column width to that but no less than 16
    hostNameLen=max(16,len(socket.gethostbyaddr(ipAddrList[-1])[0]))

    numPortContainers = defaultdict(list)
    containerCnt=containerType[:containerType.find('C')]
    print containerCnt + " containers:"
    print " ".ljust(hostNameLen),
    for port in portList:
      print str(port).rjust(5),
    print "Total".rjust(6)
    #print " "
    clusterTotal=0
    for port in portList:
      numPortContainers[port]=0
    heatMapList=[]
    for ipAddr in ipAddrList:
      #print ipAddr.ljust(16),
      heatMapLine=socket.gethostbyaddr(ipAddr)[0].ljust(hostNameLen)
      nodeTotal=0
      for port in portList:
	numContainers=nodeContainers[ipAddr][str(port)][containerType]
	numPortContainers[port]+=numContainers
	nodeTotal+=numContainers
	clusterTotal+=numContainers
        #print str(numContainers).rjust(5),
        #print str(nodeContainers[ipAddr][str(port)][containerType]).rjust(5),
        heatMapLine+=str(numContainers).rjust(6)
      #print str(nodeTotal).rjust(6)
      heatMapLine+=str(nodeTotal).rjust(7)
      heatMapList.append(heatMapLine)
      #print ""
    heatMapList.sort()
    print "\n".join(heatMapList)

    print "Total".ljust(hostNameLen),
    for port in portList:
      print str(numPortContainers[port]).rjust(5),
    print str(clusterTotal).rjust(6)

def getGlobalInfo():
  global clusterInfoJson
  global nodeInfoJson

  clusterInfoJson = getClusterInfo()
  status, description = isValidInfoJson(clusterInfoJson)
  if status != "OK":
    usage(description)

  nodeInfoJson = getNodeInfo()
  status, description = isValidInfoJson(nodeInfoJson)
  if status != "OK":
    usage(description)

def resetCounts():
  global ipAddrList
  global portList
  global nodeContainers
  global instanceList
  global volumeContainers 
  global maprMajorVersion
  
  instanceList = []
  ipAddrList = []
  portList = []
  #volumeContainerDict = defaultdict(list)
  volumeContainers = defaultdict(list)
  containerCount = {"masterCnt":0, "replicaCnt":0, "totalCnt":0, "ecstoreCnt":0}
  portContainers = defaultdict(list)

  nodeContainers = defaultdict(list)
  '''
  nodeContainers["node"]["port"]["masterCnt"]
  nodeContainers["node"]["port"]["replicaCnt"]
  nodeContainers["node"]["port"]["totalCnt"]
  '''
  
  maprMajorVersion=clusterInfoJson["data"][0]["version"][0]
  startPort=5660
  maxInstances=1
  for nodeDict in nodeInfoJson["data"]:
    maxInstances=max(maxInstances, int(nodeDict["numInstances"]))
  
  portList = range(startPort, startPort+maxInstances)

  for port in portList:
    portContainers[str(port)]=containerCount.copy()

  for nodeDict in nodeInfoJson["data"]:
    nodeIpAddrTmp = nodeDict["ip"]
    # if a node has multiple IP addresses (list instead of str), use first address
    # This logic originally assumed that the first IP address listed here is also the first IP address
    # listed for containers in maprcli dump volumeinfo when there are multiple networks:
    #                   "Master":"172.18.0.111:5660-172.19.0.111:5660--3-VALID",
    # 			"ActiveServers":{
    # 				"IP":[
    # 					"172.18.0.111:5660-172.19.0.111:5660--3-VALID",
    # 					"172.18.0.109:5663-172.19.0.109:5663--3-VALID",
    # 					"172.18.0.106:5660-172.19.0.106:5660--3-VALID"
    # I parse based on the first hyphen and use that IP address as the index into nodeContainers
    # for keeping track of container counts
    #
    # Turns out this is not always the case.  Changed logic to sort the list of addresses and then 
    # use the first one.  This works for my multiple docker network environment, but I don't know if
    # the IP's listed in maprcli dump volumeinfo are always in this sort order.  If not, it's likely
    # you'll see an error when you try to increment the count for a container into nodeContainers and the
    # node (ip) index does not exist.
    # You'll see something like 
    #   File "./volumecntrs.py", line 325, in getVolumeCntrInfo
    #     nodeContainers[node][port]["replicaCnt"]+=1
    # TypeError: list indices must be integers, not unicode
		
    if isinstance(nodeIpAddrTmp, (str, unicode)):
      nodeIpAddr=nodeIpAddrTmp
    else:
      nodeIpAddrTmp.sort()  # Here's the sort to get lexically lowest IP address
      nodeIpAddr=nodeIpAddrTmp[0]
    #print type(nodeIpAddr)
    ipAddrList.append(nodeIpAddr)
    nodeContainers[nodeIpAddr]=deepcopy(portContainers)
  ipAddrList.sort()

def getVolumeCntrInfo(volumeList,isEcStore=False):
  global maprMajorVersion
  # Create a volume container dictionary
  #   Key	: Instance
  #   Value	: List of Container Dicts

  ''' 
  The volContainerList has global volume info in the first entry.
  There is a subsequent entry for each container that looks like this.
  This is a containerDict
    {
      "ContainerId":3034,
      "Epoch":3,
      "Master":"10.10.99.64:5666--3-VALID",
      "ActiveServers":{
        "IP:Port":[
          "10.10.99.64:5666--3-VALID",
          "10.10.99.65:5666--3-VALID"
        ]
      },
      ...
    }

  For ecstore volumes (set isEcStore=True), global volume info also in first entry.
  Subsequent entry for each Container Group which has global CG info.
  Each CG entry has an ECGContainers dict containing keys cid0 through cid[data+parity-1], each
  of which has container Id and location info (where Master always equals the only IP entry)
  
  '''
  containerList=[]
  # volumeList=volumes.split(",")
  if isEcStore:
    print ""
    print "Gather Erasure Code Storage (ecstore) container information for volumes:"
  else:
    print "Gather container information for volumes:"
  for volumename in volumeList:
    print "  ",volumename
    if isEcStore:
      print "     Group   Containers"
    volumeInfoJson = getVolumeInfo(volumename)
    status, description = isValidInfoJson(volumeInfoJson)
    if status != "OK":
      usage(description)
    #containerList=volumeInfoJson["data"]
    volContainerList=volumeInfoJson["data"]
    if isEcStore:
      for volCG in volContainerList[1:]:
	print ("     "+str(volCG["ContainerGroupId"])+" ::"),
        for volCid in volCG["ECGContainers"]:
	  #print (volCid)
	  #print (volCG["ECGContainers"][volCid])
	  #print type(volCG["ECGContainers"][volCid])
	  print (str(volCG["ECGContainers"][volCid]["ContainerId"])),
	  containerList.append(volCG["ECGContainers"][volCid])
        print ""
    else:
      containerList.extend(volContainerList[1:])

    # For EC, get volume info2 and add backing store ec volume to ecvolume list
    volumeInfo2Json = getVolumeInfo2(volumename)
    status, description = isValidInfoJson(volumeInfoJson)
    if status != "OK":
      usage(description)
    if "ecstorevolume" in volumeInfo2Json["data"][0]:
      ecvolumeList.append(volumeInfo2Json["data"][0]["ecstorevolume"])
  print ""

  #for containerDict in containerList[1:]:  # Skip to container entries for Dict
  for containerDict in containerList:  
    #print type(containerDict)
    # Add Master Container to corresponding instance list
    mInstance=containerDict["Master"]
    mInstance=mInstance[:mInstance.find('-')]
    node=mInstance[:mInstance.find(':')]
    port=mInstance[mInstance.find(':')+1:]
    # print node, port
    #print json.dumps(nodeContainers)
    if isEcStore:
      nodeContainers[node][port]["ecstoreCnt"]+=1
    else:
      nodeContainers[node][port]["masterCnt"]+=1
      nodeContainers[node][port]["totalCnt"]+=1
    isMaster=True
    isValid=True # TBD extract Valid from instance
    containerId=containerDict["ContainerId"]
    #volumeContainerDict[mInstance].append(containerDict)
    volumeContainers[mInstance].append(container_fmt(containerId, isMaster, isValid, isEcStore))
    if mInstance not in instanceList:
      instanceList.append(mInstance)
    # Add additional replicas to corresponding instance lists
   
    tmpList=[]
    # If there is only 1 active server (eg 1 container and 1x replication)
    # then ["ActiveServers"]["IP:Port"] is a string, not a list.  This must
    # be a list for rInstance for loop.  Python loops through characters in a string
    # but elements in a list.

    # If MapR 6.0 or later, ActiveServers Key is "IP", vs. "IP:Port" though MapR 5.X
    activeServersKey="IP:Port"
    if int(maprMajorVersion) >= 6:
      activeServersKey="IP"

    if isinstance(containerDict["ActiveServers"][activeServersKey], (str, unicode)):
      tmpList.append(containerDict["ActiveServers"][activeServersKey])
    else:
      tmpList.extend(containerDict["ActiveServers"][activeServersKey])

    #for rInstance in containerDict["ActiveServers"][activeServersKey]:
    for rInstance in tmpList:
      isMaster=False
      rInstance=rInstance[:rInstance.find('-')]
      if rInstance == mInstance:
        continue
      node=rInstance[:rInstance.find(':')]
      port=rInstance[rInstance.find(':')+1:]
      #print node,type(node)
      #print port,type(port)
      #print type(nodeContainers)
      nodeContainers[node][port]["replicaCnt"]+=1
      nodeContainers[node][port]["totalCnt"]+=1

      #print type(volumeContainers)
      volumeContainers[rInstance].append(container_fmt(containerId, isMaster, isValid, isEcStore))
      if rInstance not in instanceList:
        instanceList.append(rInstance)
      
  instanceList.sort()

  print_containers()

  if isEcStore: # No replicas for ECG containers.  Every container is a "master".
    print ""
    print_heatmap("ecstoreCnt")
  else:
    for countType in ["masterCnt","replicaCnt","totalCnt"]:
      print ""
      print_heatmap(countType)

def main():

  if len(sys.argv) != 2:
    usage()
  
  # volumename=sys.argv[1]
  # Comma separated list of volumes
  global volumes
  volumes=sys.argv[1]
  
  getGlobalInfo()
  resetCounts()

  global volumeList
  global ecvolumeList
  ecvolumeList=[]
  volumeList=volumes.split(",")
  getVolumeCntrInfo(volumeList)
  
  # Now add getting and printing ecVolumeCntrInfo here if there were any EC volumes in original volumeList
  if ecvolumeList:
    resetCounts()
    getVolumeCntrInfo(ecvolumeList,True)

if __name__ == "__main__":
   main()

exit(0)
