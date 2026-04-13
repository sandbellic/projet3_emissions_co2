/* suppression des doublons
et renommage des colonnes liées à latitude et longitude + tranformation en float
A noter les gares sont celles de Fr métropolitaine sans la Corse*/


with gares as (
   SELECT distinct on (code_uic)           /* distinct on => spécifique postgre pour trouver les doublons*/
        code_uic, commune, libelle, "c_geo.lat", "c_geo.lon", 
        trim(lower(unaccent(replace(replace(replace (lower(commune), '–', '-'),'ç', 'c'),'-', ' ')))) as commune_clean,
        trim(lower(unaccent(replace(lower(departemen),'-', ' ')))) as departement
    FROM {{ source('emissions_co2', 'raw_gares') }} 
    order by code_uic,"c_geo.lat", "c_geo.lon"
),

departement as (
    select code_departement,
    trim(lower(replace(unaccent(nom_departement), '-', ' '))) as departement
    FROM {{ source('emissions_co2', 'raw_departements') }} 
),

join_gares_departements as (
    select 
        g.code_uic,
        g.commune,
        g.commune_clean,
        g.libelle as nom_gare,
        d.code_departement,
        "c_geo.lat"::float AS latitude_deg,
        "c_geo.lon"::float AS longitude_deg
    FROM gares g
    left join departement as d on g.departement = d.departement

)


SELECT * FROM join_gares_departements
