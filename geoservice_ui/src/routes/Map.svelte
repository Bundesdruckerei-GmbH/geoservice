<script>
  import { onMount } from "svelte";
  import { browser } from "$app/environment";

  let wholemap;
  let spatialData_geoJson;
  let zoom_level = 6;

  async function loadgeom(zoom, bbox) {
    return fetch(
      `http://127.0.0.1:5000/api/geo/?filter_boundingbox_southwest_lat=${bbox._southWest.lat}&filter_boundingbox_southwest_lng=${bbox._southWest.lng}&filter_boundingbox_northeast_lat=${bbox._northEast.lat}&filter_boundingbox_northeast_lng=${bbox._northEast.lng}&zoom_level=${zoom}`
    )
      .then((response) => response.json())
      .catch((error) => {
        console.log(error.message);
        alert(
          "Fehler beim Laden der Ländergeometrien. Bitte die App neu starten oder den Maintainer informieren."
        );
      });
  }

  function drawmap(map) {
    zoom_level = map.getZoom();
    loadgeom(zoom_level, map.getBounds()).then((geojsonobj) => {
      if (spatialData_geoJson) {
        map.removeLayer(spatialData_geoJson);
      }
      spatialData_geoJson = L.geoJson(geojsonobj, {
        style: (feature) => {
          return {
            fillColor: "#00ff84",
            weight: 1,
            color: "black",
            fillOpacity: 0.5,
          };
        },
      });
      spatialData_geoJson.addTo(map);
    });
  }

  function createMap() {
    let map;
    map = L.map(wholemap, {
      attributionControl: false,
    }).setView([51, 10], zoom_level);

    var osm = L.tileLayer(
      "https://tile.openstreetmap.de/{z}/{x}/{y}.png",
      {
        maxZoom: 15,
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright/"  target="_blank">OpenStreetMap</a> ' +
          '(<a href="https://opendatacommons.org/licenses/odbl/"  target="_blank">ODbL</a>)',
      }
    ).addTo(map);

    // Add GADM attribution
    L.control
      .attribution({
        prefix:
          '<a href="https://leafletjs.com/">Leaflet</a> | ' +
          '<a href="https://gadm.org/"  target="_blank">GADM</a>',
      })
      .addTo(map);

    drawmap(map);

    var overlayMaps = {
      OSM: osm,
    };

    var layerControl = L.control.layers(null, overlayMaps).addTo(map);

    // create leaflet object for the countryData

    map.on("zoomend", () => drawmap(map));

    map.on("moveend", () => drawmap(map));

    return map;
  }

  onMount(async () => {
    if (browser) {
      const L = await import("leaflet");
      createMap();
    }
  });
</script>

<div class="bigmap">
  <div id="map" bind:this={wholemap}></div>
</div>

<style>
  @import "leaflet/dist/leaflet.css";
  #map {
    width: 100%;
    height: 100%;
    background-color: #fff;
  }

  .bigmap {
    width: 100%;
    height: 95vh;
    position: relative;
  }
</style>
