#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#check for installation of parallel-rsync
if [ ! -e /usr/bin/parallel-rsync ] ; then
echo "You do not appear to have installed: parallel-rsync"
echo "sudo aptitude install pssh"
exit
fi

#load config parameters
source deployment.properties

#build the distribution
source ./internals/build_distro.sh

#upload the distribution
source ./internals/upload_distro.sh


exit 0



