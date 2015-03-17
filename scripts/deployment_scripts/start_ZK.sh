#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties
ssh root@cloud1.sics.se rm -rf /tmp/zookeeper
ssh root@cloud1.sics.se mkdir -p /tmp/zookeeper
ssh root@cloud1.sics.se /root/zookeeper-3.3.6/bin/zkServer.sh start





