/* calcule un durée estimée de vol entre 2 aéroports */
/* on part du fait qu'un avion met environ 30min entre le décollage et l'attérissage et qu'il vole à 800 km/h) */

{% macro duree_avion_min(distance_km) %}
    ROUND((({{ distance_km }} / 800.0) + 0.5) * 60)
{% endmacro %}