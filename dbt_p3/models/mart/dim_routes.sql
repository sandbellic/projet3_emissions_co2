{{ config(
    materialized='table',
    indexes=[
      {'columns': ['id_commune_departure']},
      {'columns': ['id_commune_arrival']},
      {'columns': ['id_commune_departure', 'id_commune_arrival']}
    ]
) }}

with emissions_cO2 as (
    select *
    from {{ref('stg_emission_co2_par_transport')}}

),

air_routes as (
    select 
        'Avion trajet court' as type_transport,
        id_commune_source as id_commune_departure,
        nom_airport_source as name_departure,
        id_commune_destination as id_commune_arrival,
        nom_airport_destination as name_arrival,
        distance_km,
        duree_min,
        (select part_transport
        from emissions_co2
        where mode_transport = 'Avion trajet court') as emission_transport, 
        (select part_fabrication
        from emissions_co2
        where mode_transport = 'Avion trajet court') as emission_fabrication
    from {{(ref('stg_air_distances'))}}
),

train_routes as (
    select
        type_transport,
        id_commune_departure,
        stop_name_departure as name_departure,
        id_commune_arrival,
        stop_name_arrival as name_arrival,
        distance_km,
        (EXTRACT(EPOCH FROM duree_trajet) / 60)::int AS duree_min ,
        case 
            --- cas particulier du TER
            when type_transport = 'TER' then 19.4
            else (select part_transport from 
                    emissions_co2
                    where mode_transport = type_transport)
        end as emission_transport, 
        case 
            --- cas particulier du TER
            when type_transport = 'TER' then 4.5
            else (select part_fabrication from 
                    emissions_co2
                    where mode_transport = type_transport)
        end as emission_fabrication
    from {{(ref('stg_train_routes'))}}   
)


select * 
from air_routes
union all
select *
from train_routes
