/*on va garder uniquement les routes aériennes dont aéroport de départ, et aéroport arrivée sont 
dans la liste des aéroports francais retenus dans stg_airports */

with source as (
    select * from {{ source('emissions_co2', 'raw_routes_air') }}
),
renamed as (
    select
        "Source_airport" as source_airport, 
        "Destination_airport" as destination_airport
    from source
),

/*on ne retient que les routes intérieures France*/
routes_fr as (select r.* 
from renamed as r
inner join {{ ref( 'stg_airports' )}}  a
    on lower(r.source_airport) = lower(a.iata_code)
inner join {{ ref( 'stg_airports' )}}  b
    on lower(r.destination_airport) = lower(b.iata_code)
)

/*on s'assure qu'on n'a pas de doublons*/

select distinct on (source_airport, destination_airport)
    *
from routes_fr
