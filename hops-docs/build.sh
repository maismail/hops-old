#!/bin/bash
set -e
mvn clean docbkx:generate-pdf

#pdftk target/docbkx/pdf/userguide.pdf cat 3-end output target/docbkx/pdf/userguide-sm.pdf
#pdftk D2_1_coversheet.pdf target/docbkx/pdf/userguide-sm.pdf cat output d21-bbc-kth.pdf
pdftk D2_1_coversheet.pdf target/docbkx/pdf/userguide.pdf cat output d21-bbc-kth.pdf
evince d21-bbc-kth.pdf&
