/* on va charger les 2 raw liées aux émissions co2 par type de transport, avec et sans la partie fabrication
on va faire une seule vue pour calculer la part de la fabrication au km/personne et la part lié au transport pur */

with avec as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2') }}
),

sans as (
    select *
    from {{ source('emissions_co2', 'raw_valeur_emissions_co2_sans') }}
),

 final as (
    select a.id, a.name as mode_transport, s.value as part_transport, (a.value - s.value) as part_fabrication
    from avec a
    inner join sans s on a.id = s.id
 )

 select * from final
