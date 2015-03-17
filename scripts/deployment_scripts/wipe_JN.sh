#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

ssh root@cloud3.sics.se rm -rf $HOP_Dist_Folder/journal
ssh root@cloud4.sics.se rm -rf $HOP_Dist_Folder/journal
ssh root@cloud5.sics.se rm -rf $HOP_Dist_Folder/journal




