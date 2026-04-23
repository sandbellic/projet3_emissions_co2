with sans_coordonnees as (
    select * from {{ref('coordonnees_villes_manquantes')}}
),

correction as (
    select 
        c.commune as name,
        c.population,
        c.id_commune, 
        c.dep_code,
        case 
            when c.latitude is null then sc.latitude
            else c.latitude
        end as latitude_centre,
        case 
            when c.longitude is null then sc.longitude
            else c.longitude
        end as longitude_centre
    from {{ref('stg_communes')}} c
    left join sans_coordonnees sc on sc.ville = c.commune
)



select *
from correction
order by name