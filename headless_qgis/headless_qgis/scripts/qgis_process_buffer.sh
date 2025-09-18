#! /bin/bash

INFILENAME="$1"
DISTANCE="$2"
OUTFILENAME=/tmp/result.gpkg

export QT_QPA_PLATFORM=offscreen

qgis_process run native:buffer -- INPUT="$INFILENAME" DISTANCE="$DISTANCE" OUTPUT="$OUTFILENAME" > /dev/null

cat "$OUTFILENAME"