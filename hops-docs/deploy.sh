#!/bin/bash
mvn clean docbkx:generate-html
scp -r target/docbkx/generated/html jdowling@jimdowling.info:/var/www/hops/sites/default/files/html/

mvn clean docbkx:generate-pdf
# mvn clean docbkx:generate-webhelp
