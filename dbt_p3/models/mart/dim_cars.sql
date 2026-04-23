with emissions_cO2 as (
    select mode_transport, part_fabrication, part_transport
    from {{ref('stg_emission_co2_par_transport')}}

),

cars as (
    select 
        categorie_masse, type_puissance, type_energie,
        median_co2 as emission_transport,
    case 
        when type_energie in ('Essence', 'Diesel', 'Autre') then
                (select part_fabrication from emissions_co2 
                where mode_transport = 'Voiture thermique' )
/*        when type_energie in ('Hybride', ) then.  */
        when type_energie in ('Hybride') then    
                (select part_fabrication from emissions_co2 
                where mode_transport = 'Voiture électrique' )
        else 0 
    end as emission_fabrication
    from {{ref('stg_cars')}}
), 

electric_cars as (
    select *
    from {{ref('cars_manquants')}}
)

select * from electric_cars
union all
select * from cars