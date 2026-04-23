
/* a partir de stg_routes_air et de stg_communes_airports, on va compléter les routes avec 
les noms des aéroports, leur commune de rattachement, et on va calculer
 la distance entre les aéroports source et destination grace à la formule de Hersine*/
with source as (
    select * from {{ ref( 'stg_air_routes' ) }}
),

calcul_distance as (
select a.id_commune as id_commune_source,  a.nom_airport as nom_airport_source, 
        b.id_commune as id_commune_destination,  b.nom_airport as nom_airport_destination,
    {{ haversine_km('a.latitude_deg', 'a.longitude_deg', 'b.latitude_deg', 'b.longitude_deg') }} as distance_km  
from source as s
inner join {{ ref( 'stg_communes_airports' )}}  a
    on lower(s.source_airport) = lower(a.iata_code)
inner join {{ ref( 'stg_communes_airports' )}}  b
    on lower(s.destination_airport) = lower(b.iata_code)
)

select * ,
    {{ duree_avion_min('distance_km') }} as duree_min
from calcul_distance
