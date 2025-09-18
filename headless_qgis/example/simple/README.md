# Examples

## Buffer

Nutzt den Endpunkt `/qgis/buffer` um die übergebene Geometrie zu Buffern.
Aufruf via

    curl \
        -X POST \
        -F file=@Berlin.gpkg \
        https://localhost/qgis/buffer?distance=0.01 \
        -o /tmp/buffered_berlin.gpkg

## Script

Nutzt den Endpunkt `/qgis/print`, um das übergebene Skript+Payload auszuführen.
Aufruf via

    curl \
        -X POST \
        -F script=@qgis_print.py \
        -F data=@layout.qgz \
        -F "data=@deu_ne_adm1 1.gpkg" \
        https://localhost/qgis/print \
        -o /tmp/print.png