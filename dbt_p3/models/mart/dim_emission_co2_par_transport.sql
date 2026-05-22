-- dans un premier temps on va transformer TGV, Intercités et TER en train
-- puis on va généraliser : air, rail, route

with first as  (
    select 
        id,
        case
            when mode_transport in ('TGV', 'Intercités', 'TER') then 'Train'
            else mode_transport
        end as mode_transport,
        taille,
        case
            when mode_transport in ('TGV', 'Intercités') then mode_transport
            else detail
        end as detail,
        part_transport,
        part_fabrication

    from {{ ref('stg_emission_co2_par_transport') }}
)

select 
    id, mode_transport, taille, detail, part_fabrication, part_transport,
    case 
        when lower(mode_transport) like '%avion%' then 'air'
        when lower(mode_transport) like '%train%' then 'rail'
        else 'route'
    end as type_transport

from first