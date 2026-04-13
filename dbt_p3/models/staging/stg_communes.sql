/*on va garder au moins dans un premier temsp uniquement les villes de plus de 10000 habitants => environ 1000 communes
on garde aussi les colonnes nom_standard , population , latitude_centre, longitude_centre ,
et les communes situées en France métropolitaine lat/long*/

with source as (
    select nom_standard, 
        trim(lower(unaccent(replace(lower(replace(nom_standard, '-', ' ')), 'ç', 'c')))) AS commune_clean,
        population, dep_code, latitude_centre, longitude_centre 
    from {{ source('emissions_co2', 'raw_communes') }}
    where population >= {{ var('population_commune') }} 
            and latitude_centre between 41.3 and 51.1
            and longitude_centre between -5.1 and 9.6

),
renamed as (
    select
        ROW_NUMBER() OVER (ORDER BY nom_standard) AS id_commune,
        nom_standard as commune, 
        commune_clean,
        population, 
        dep_code,
        latitude_centre as latitude,
        longitude_centre as longitude
    from source
)
select * from renamed