/*--- A partir des stops en gare , on va les lier aux communes afin de récupérer les identifiants id_commune
-- dans les stops on va garder uniquement les colonnes qui sont utiles et les stop_id interssants 
--- ensuite on clean le nom de l'arrêt (en 2 temps)
---- ensuite on rapproche de commune et on récupère l'identifiant
*/

with stops as (
    select stop_id, stop_name,stop_lat, stop_lon
    from {{ source('emissions_co2', 'raw_stops') }}
    where stop_id like 'StopPoint%'

),

stops_fist_clean as (

    select stop_id, stop_name,stop_lat, stop_lon,
            case 
           ----cas de Le Touquet, La Plagne, ...
            when stop_name ~* '^(le |la |les |ker )' then
                ----intermediaire = SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1)
                trim(SPLIT_PART(stop_name, ' ', 1) || ' ' || SPLIT_PART(stop_name, ' ', 2))
            when lower(stop_name) ~* '^(saint |sainte )' then           
                trim(SPLIT_PART(stop_name, ' ', 1) || ' ' || SPLIT_PART(stop_name, ' ', 2))
            when lower(stop_name) ~* '^(saint-|sainte-)' then           
                trim(SPLIT_PART(stop_name, '-', 1) || '-' || SPLIT_PART(SPLIT_PART(stop_name, '-', 2), ' ',1))                         
            ------dans tous les autres cas , on prend tout avant espace OU tiret
            else trim(SPLIT_PART(stop_name, ' ', 1))
        end as name_clean        
    from stops

),

stops_name_cleaned as (
    select stop_id, stop_name, stop_lat, stop_lon,
        trim(replace(lower(unaccent(name_clean)), '-', ' ')) as stop_name_clean
    from stops_fist_clean
),

stops_gares as (
    select s.*, g.code_departement
    from stops_name_cleaned s
    inner join {{ref('stg_gares')}} g on g.commune_clean = stop_name_clean

),

stops_communes as (
    select c.id_commune, 
        stop_id, stop_name, stop_lat, stop_lon
    from stops_gares sg
    inner join {{ref('stg_communes')}} c on c.commune_clean = sg.stop_name_clean and sg.code_departement = c.dep_code

)


select  DISTINCT ON (stop_id) *
from stops_communes
