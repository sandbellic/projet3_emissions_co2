/* A partir des routes, on va enlever celles présentes mais non définies (short_name = 'INCONNU') et garder uniquement
les routes liées aux trains (route_type = 2) - pour info présent également de route_type = 0=> tramway et =3=> Bus*/

/* routes = Itinéraire, Trip = voyage, headsign = destination du voyage
une même route peut donner lieu à plusieurs trips, un trip peut donner lieu à plusieurs services(jours de désertes)*/

with routes as (
    select route_id, route_short_name, route_long_name
    from {{ source('emissions_co2', 'raw_routes_train') }} 
    where route_short_name <> 'INCONNU' and route_type = 2
),


join_trips as (
    select r.* , 
        t.trip_id, t.direction_id, t.trip_headsign, 
        split_part(t.trip_id,':',2) as type_transport
    from routes  r
    inner join {{ source('emissions_co2', 'raw_trips') }}  t on t.route_id = r.route_id
    where split_part(t.trip_id,':',2) in ('OUI', 'OGO', 'IC', 'ICN', 'TER')
), 

join_stop_times as (
    select tst.*,
        jt.direction_id, jt.trip_headsign, 
        case 
            when type_transport in ('OGO','OUI') then 'TGV'
            when type_transport in ('IC', 'ICN') then'Intercités'
            else 'TER' 
        end as type_transport
    from {{ref('stg_train_stop_times')}} as tst 
    inner join join_trips as jt on jt.trip_id = tst.trip_id
)

select * from join_stop_times



/*

dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY trip_id, seq_departure, id_commune_departure, stop_name_departure,
                                    seq_arrival, id_commune_arrival, stop_name_arrival,
                                    attente_gare, duree_trajet, distance_km, direction_id, trip_headsign, type_transport
               ORDER BY trip_id
           ) AS rn
    FROM join_stop_times
)
DELETE FROM {{ ref('stg_train_stop_times') }}
WHERE ctid IN (
    SELECT ctid
    FROM dedup
    WHERE rn > 1
);
*/

