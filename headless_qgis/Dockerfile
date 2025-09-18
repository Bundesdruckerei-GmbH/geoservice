FROM qgis/qgis:latest

RUN apt-get update \
 && apt-get install -y \
        python3-flask \
 && apt-get clean

COPY headless_qgis /headless_qgis

ENV PATH="/headless_qgis/scripts:${PATH}"

ENTRYPOINT [ "flask", "--app", "headless_qgis.app:app", "run", "--port=8080", "--host=0.0.0.0" ]