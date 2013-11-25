#!/bin/bash
mvn clean docbkx:generate-pdf
pdftk D2_1_coversheet.pdf target/docbkx/pdf/userguide.pdf cat output d21-bbc-kth.pdf
