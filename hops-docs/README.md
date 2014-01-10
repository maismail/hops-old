To build the docbook, run:

mvn docbkx:generate-html

and/or

mvn docbkx:generate-pdf

and/or

mvn clean docbkx:generate-webhelp


Then:

scp -r target/docbkx/generated/html jdowling@jimdowling.info:/var/www/hops/sites/default/files/html/
