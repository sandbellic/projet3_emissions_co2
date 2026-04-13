/* on reprend nos stops avec identifiant commune
---- on va le join avec les stop_times
----- !!!!on ne garde pour le moment que les trajets TGV, TER et intercités (jour, nuit)
---- le join va nous permettre de calculer les durées des trajets entre les gares successives (stop_sequence)
---- pour cela on va utiliser le LAG et regrouper par trip_id, pour calculer la durée avec le stop_sequence suivant
---- on profite aussi pour calculer via la méthode de Haversine kilométrage (à vol d'oiseau) entre 2 stop_sequence successifs

-----  {{ haversine_km('stop_lat', 'stop_lon', 'LAG(stop_lat)', 'LAG(stop_lon)') }} OVER (PARTITION BY trip_id ORDER BY stop_sequence) as distance_km , 

*/
with stop_times as(
    select trip_id,  
        arrival_time::interval as arrival_interval, 
        departure_time::interval as departure_interval, 
        stop_id, stop_sequence
   from {{ source('emissions_co2', 'raw_stop_times') }} 
   where split_part(trip_id,':',2) in ('OUI', 'OGO', 'IC', 'ICN', 'TER')
),

join_stops as (
    select st.trip_id, st.arrival_interval, st.departure_interval, st.stop_id,
        st.stop_sequence, s.id_commune, s.stop_name, s.stop_lat, s.stop_lon
    from stop_times st
    join {{ref('stg_train_stops')}}  s on s.stop_id = st.stop_id
),


calcul_tps_distance as (
    select trip_id, 
        (LAG(stop_sequence) OVER (PARTITION BY trip_id ORDER BY stop_sequence)) as seq_departure,
        (LAG(id_commune) OVER (PARTITION BY trip_id ORDER BY stop_sequence)) as id_commune_departure,       
        (LAG(stop_name) OVER (PARTITION BY trip_id ORDER BY stop_sequence)) as stop_name_departure,              
        stop_sequence as seq_arrival,
        id_commune as id_commune_arrival,
        stop_name as stop_name_arrival,
        departure_interval - arrival_interval as attente_gare,
        (arrival_interval - LAG(departure_interval) OVER (PARTITION BY trip_id ORDER BY stop_sequence)) as duree_trajet,
        ({{ haversine_km('stop_lat', 'stop_lon',
            'LAG(stop_lat) OVER (PARTITION BY trip_id ORDER BY stop_sequence)',
            'LAG(stop_lon) OVER (PARTITION BY trip_id ORDER BY stop_sequence)')}}) as distance_km 

    from join_stops
    order by trip_id, stop_sequence
)

select * 
from calcul_tps_distance
where seq_departure is not null