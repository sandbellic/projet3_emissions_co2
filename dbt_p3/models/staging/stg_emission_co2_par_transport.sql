/* on va charger les 2 raw liées aux émissions co2 par type de transport, avec et sans la partie fabrication
on va faire une seule vue pour calculer la part de la fabrication au km/personne et la part lié au transport pur */
/* après vérification le champ value dans raw_valeur_emissions_co2' correspond aux émissions liées à l'usage
et les valeurs dans raw_valeur_emissions_co2'_sans aux émissions totales (usage + fabrication) */


with usage as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2') }}
),

global as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2_sans') }}
),

 final as (
    select a.id, a.name as mode_transport, a.value as part_transport, (s.value - a.value) as part_fabrication
    from usage  a
    inner join global s on a.id = s.id
 )

 select * from final
