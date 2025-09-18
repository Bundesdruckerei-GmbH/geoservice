#! /bin/bash

DATADIR=$1
OUTFILE=$2

export QT_QPA_PLATFORM=offscreen

python3 "$DATADIR/script.py" "$DATADIR" "$OUTFILE"
