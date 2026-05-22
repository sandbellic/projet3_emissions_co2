/* on va charger les 2 raw liées aux émissions co2 par type de transport, avec et sans la partie fabrication
on va faire une seule vue pour calculer la part de la fabrication au km/personne et la part lié au transport pur */
/* après vérification le champ value dans raw_valeur_emissions_co2' correspond aux émissions liées à l'usage
et les valeurs dans raw_valeur_emissions_co2'_sans aux émissions totales (usage + fabrication) */


with usage as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2_usage') }}
    where name not like 'Covoiturage%'
),

global as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2_global') }}
    where name not like 'Covoiturage%'
),

intermediare as (
    select u.id, u.name, u.value as part_transport, (g.value - u.value) as part_fabrication
    from usage as u
    inner join global g on u.id = g.id
),

final as (

   select id, 
    case 
        when name like '%(%'        
            then trim(SPLIT_PART(name, '(', 1)) 
        else trim(name)
    end as mode_transport,
    
    case 
        when name like '%(%'
            then trim(SPLIT_PART(
                    SPLIT_PART(
                        SPLIT_PART(name, '(', 2), 
                            ')',1),
                    '-', 1)
                ) 
        else ''
    end as taille,
                
    case
        when name like '%-%'
            then trim(SPLIT_PART(
                    SPLIT_PART(
                        SPLIT_PART(name, '(', 2), 
                            ')',1),
                    '-', 2)
                ) 
        else ''
    end as detail,
    part_transport,  part_fabrication
    from intermediare
 )

 select * 
 from final
