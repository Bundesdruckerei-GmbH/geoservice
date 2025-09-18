# LANDSCAN

## Overview

## Curl-Command Example

Copy:

curl -X POST -F script=@.\qgis_print.py -F data=@.\ne_adm0.gpkg -F data=@.\ne_10m_populated_places.gpkg -F data=@.\landscan-global-2023-colorized_small.tif -o print.png https://localhost/qgis/print 


    curl -X POST \
         -F script=@./qgis_print.py \
         -F data=@./ne_adm0.gpkg \
         -F data=@./ne_10m_populated_places.gpkg \
         -F data=@./landscan-global-2023-colorized_small.tif \
         -o ./print.png \
         https://localhost/qgis/print
