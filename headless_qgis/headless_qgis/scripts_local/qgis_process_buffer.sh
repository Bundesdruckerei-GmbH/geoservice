#! /bin/bash

TEMPDIR=$(mktemp -d)

cp $1 $TEMPDIR/in.gpkg
distance=$2

docker run --rm \
    -v $TEMPDIR:/data \
    -e QT_QPA_PLATFORM=offscreen \
    --entrypoint=qgis_process \
    qgis/qgis  \
    run native:buffer -- INPUT=/data/in.gpkg DISTANCE=$distance OUTPUT=/data/result.gpkg > /dev/null
cat $TEMPDIR/result.gpkg
