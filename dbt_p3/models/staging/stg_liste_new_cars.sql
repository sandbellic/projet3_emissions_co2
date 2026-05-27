
    -- les noms non quotés sont automatiquement convertis en minuscule, le mieux est de leur mettre un alias    
with liste_new_cars as (
    select carrosserie, modele, energie, co2_min, co2_max,
        ("co2_min" + "co2_max") / 2 as co2_moyen
    from {{ source('emissions_co2', 'raw_new_cars') }}
    where "energie" is not null    
)

select 
    carrosserie, modele, energie, 
    case
        when lower(carrosserie) = 'coupé / cabriolet' then 'Petite'
        when lower(carrosserie) = 'berline' then 'Berline'
        when lower(carrosserie) in ('break', 'monospace') then 'Moyenne'
        when lower(carrosserie) in ('tout-terrains', 'minibus') then 'SUV'
        else 'Autre'
    end as categorie,
    co2_min, co2_max, co2_moyen

from liste_new_cars

