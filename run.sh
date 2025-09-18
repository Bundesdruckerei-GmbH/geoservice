#!/bin/bash

GEOSERVICE_WEBWORKER_AMOUNT="${GEOSERVICE_WEBWORKER_AMOUNT:-8}"
GEOSERVICE_WEBWORKER_TIMEOUT="${GEOSERVICE_WEBWORKER_TIMEOUT:-5}"

case "$1" in

    ""|"webserver")
        gunicorn \
            --access-logfile - \
            --timeout "$GEOSERVICE_WEBWORKER_TIMEOUT" \
            --workers "$GEOSERVICE_WEBWORKER_AMOUNT" \
            --bind 0.0.0.0:8080 \
            geoservice:app
    ;;

    "worker")
        flask work
    ;;

    "migrate")
        flask db upgrade
    ;;

    "cli")
        flask "${@:2}"
    ;;

    *)
      echo "unknown role '$1'"
    ;;
esac
