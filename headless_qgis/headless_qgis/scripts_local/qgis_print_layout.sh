#! /bin/bash

DATADIR=$1
OUTFILE=$2

docker run --rm \
    -v $DATADIR:/data \
    -e QT_QPA_PLATFORM=offscreen \
    --entrypoint=python3 \
    qgis/qgis \
    /data/script.py /data /data/out

ls -l $DATADIR >&2

if [ "$DATADIR/out" != "$OUTFILE" ]; then
    cp $DATADIR/out $OUTFILE
fi