import streamlit as st
import folium
from streamlit_folium import st_folium

# ------------------
# AFFICHAGE CARTE GEO
# ------------------
def render_map(coords, distance_route_value):

    st.markdown('<div class="section-title">🗺️ Trajet par routes', unsafe_allow_html=True)
    st.success(f"📏 Distance route réelle : {distance_route_value:.1f} km")
 

    coords_latlon = [(c[1], c[0]) for c in coords]

    m = folium.Map(location=coords_latlon[0], zoom_start=6)
    folium.PolyLine(coords_latlon, color="blue", weight=4).add_to(m)
    folium.Marker(coords_latlon[0], tooltip="Départ").add_to(m)
    folium.Marker(coords_latlon[-1], tooltip="Arrivée").add_to(m)

    st_folium(m, width=700, height=400, returned_objects=[])
