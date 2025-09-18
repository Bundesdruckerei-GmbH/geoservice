FROM alpine:latest

# Upgrade system packages to address vulnerabilities
RUN apk add --no-cache python3 py3-pip python3-dev
RUN apk add --no-cache \
    gcc gfortran build-base openblas-dev freetype-dev \
    proj proj-dev proj-util geos geos-dev gdal gdal-dev

RUN mkdir /app
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["/app/run.sh"]

ENV TZ="Europe/Berlin"
ENV GEOSERVICE_DEBUG="False"
ENV FLASK_APP="geoservice:app"

COPY --chmod=755 run.sh /app/run.sh

COPY requirements.txt /app/
RUN python3 -m venv /app/venv
RUN /app/venv/bin/pip install -r requirements.txt

COPY resources /app/resources
COPY geoservice /app/geoservice

RUN apk add --no-cache bash
ENV PATH=/app/venv/bin:$PATH
