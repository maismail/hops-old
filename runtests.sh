#!/bin/bash

for i in $(cat all_tests.txt); do
 echo "mvn -Dtest=$i test"
       mvn -Dtest=$i test
 OUT=$?
 if [ $OUT -eq 0 ]; then 
   echo "$i PASSED" >> TestResults
 else
   echo "$i FAILED" >> TestResults
 fi
 killall java
 sleep 5
 find . -iname "test.log" -exec rm -f {} \;
done

