#!/bin/bash

username=$1

filename=$2

perl -pi -e 's/'$3'/'$4'/g' $filename

exit 0
