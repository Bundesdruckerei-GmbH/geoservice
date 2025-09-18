FROM python:3.9-slim-buster

ARG PIP_EXTRA_INDEX_URL

WORKDIR /
EXPOSE 80

ENV TZ="Europe/Berlin"
ENV GEOSERVICE_DEBUG="False"
ENV FLASK_APP="geoservice:app"

COPY run.sh /
RUN chmod ugo+x /run.sh

COPY requirements.txt /tmp/
RUN pip3 install --requirement /tmp/requirements.txt

COPY geoservice /geoservice

COPY resources/data.txt /data/data.txt
ENTRYPOINT ["/run.sh", "cli", "etljob", "/data/data.txt"]