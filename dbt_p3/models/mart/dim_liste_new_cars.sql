with emissions_cO2 as (
    select mode_transport, part_fabrication, part_transport
    from {{ref('stg_emission_co2_par_transport')}}

),

liste_new_cars as (
    select 
        carrosserie, energie,
        co2_moyen as emission_transport,
    case 
        when energie in ('Essence', 'Diesel', 'Autre') then
                (select part_fabrication from emissions_co2 
                where mode_transport = 'Voiture thermique' )
        when energie in ('Hybride','Electrique') then    
                (select part_fabrication from emissions_co2 
                where mode_transport = 'Voiture électrique' )
        else 0 
    end as emission_fabrication
    from {{ref('stg_liste_new_cars')}}
) 


select * from liste_new_cars