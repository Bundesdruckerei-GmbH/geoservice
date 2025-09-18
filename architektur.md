# Geoservice

## Einführung und Ziele

Mit dem Geoservice wird einen eigenständiger Backend-Dienst geboten, mit dem sich Entwickler und Analysten Geoinformationen für ihre Applikationen beschaffen können. Es werden verschiedenste Ausgabeformate unterstützt um die 3 Datenkategorien Geometrien, Raster und Attribute zu liefern.


### Aufgabe der Applikation

- Persistierung verschiedenster GEO-Datensätze mit einer ETL-Strecke
- Bereitstellung eines REST-Services zur Konsumierung der GEO-Datensätze
- Bereitstellung einer Frontend-Komponente zur automatisierten Anbindung des REST-Services


### Nutzung und Support

- User-Dokumentation ist Teil der Laufzeitumgebung und kann über die Frontpage erreicht werden
- Support & Kommunikationskanäle: Bitte per Feature-Requests über Github


## Relevante Entwurfs- und Architekturentscheidungen

- Backend-Technologie: Python
- Persistenz-Technologie: PostGIS
- Frontend-Technologie: JavaScript / Leaflet


## Bausteinsicht

### OSS-Backend

Enthält das Python-Backend für den Service, den Zugriff auf die Datenbank und das Framework für die ETL-Strecke.

### OSS-Frontend

Enthält die JS-Bibliothek um ein Kartenfrontend wie OpenLayer oder Leaflet an den GeoService-Backend anzubinden.

### Datenbank

PostGIS-Datenbank. Grundsätzlich werden die Daten mit Bibliotheken wie GeoPandas oder MapShaper aufbereitet und in die DB eingespielt.
Die DB enthält die Daten abfragenoptimiert. Die Live-Geofunktionen beim Abruf werden durch die PostGIS-spezifischen Befehle abgedeckt.

### Sonstige Schnittstellen

Alle ETL-Prozesse gehen von abgelegten Daten in lokalen Verzeichnissen aus. Extraction ist also auf lokale Resourcen beschränkt.
Die Transformationen sind pro Datenquelle unterhalb von geoservice/controller/data_sources zu finden.


## Querschnittliche Konzepte

### Lizenzen

- OSS-Komponenten allesamt unbedenklich
- PostGIS-DB: GPL, steckt die Anwendung nicht an
- Kartendatensätze: Im Einzelfall zu betrachten, steckt die Anwendung nicht an


### Monitoring

Der Geoservice stellt unter der relativen URL /monitoring/ einen Integrationstest als Healthcheck bereit.

Im Erfolgsfall wird der Statuscode 200 zurückgegeben. Im Fehlerfall wird Statuscode 500 zurückgegeben und im Rückgabe-Body sind
die fehlgeschlagenen Tests aufgeführt.

Dieser Endpunkt kann im Standard-Monitoring-Werkzeug konfiguriert werden und die Funktionsfähigkeit der Anwendung kann damit
proaktiv überwacht werden.


## Entwicklung & Betrieb

### Einrichten der Entwicklungsumgebung

- Repository Clone
- Folgen der Anleitung unter README.md
