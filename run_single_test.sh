#!/bin/bash

TestName=$1
ResultFile=$2

 echo "mvn -Dtest=$TestName test"
       mvn -Dtest=$TestName test
 OUT=$?	
 
 if [ $OUT -eq 0 ]; then 
   echo "$TestName PASSED" >> $ResultFile
 else
   echo "$TestName FAILED" >> $ResultFile
 fi





