/*on va garder au moins dans un premier uniquement les aéroports large et medium,
        ainsi que ceux ayant un code OACI et IATA non null */
/*  les noms des aéroports ne sont pas normalisés, on va distinguer plusieurs cas
            - tout ce qui commence par paris est paris
            - tout ce qui contient mulhouse est mulhouse
            - tout ce qui commence par le la les saint saintes saint- saintes : on garde le nom d'après
            - dans tous les autres cas , on prend tout avant espace OU tiret  */
/* Objectif : pouvoir les rattacher à un nom de commune , puisque l'utilisateur va faire son choix en fonction des communes */ 

with source as (
    select icao_code, iata_code,
        trim(lower(municipality)) as commune,
        name as nom_airport,latitude_deg, longitude_deg,  type,
        trim(replace(replace(lower(name), '–', '-'), 'airport', '')) as name_clean
    from {{ source('emissions_co2', 'raw_airports') }}
    where type in ('large_airport', 'medium_airport') and icao_code is not null and iata_code is not null

),
cleaned as (
    select
        icao_code,
        iata_code,
        commune,
        ------les noms des aéroports ne sont pas normalisés, on va distinguer plusieurs cas
        case 
            ----- tout ce qui commence par paris est paris
            when commune ~ '^paris' then 'paris'
            -----ce qui contient mulhouse est mulhouse
            when commune like '%mulhouse%' then'mulhouse'
            ----cas de Le Touquet, La Plagne, ...
            when commune ~ '^(le|la|les)' then
                ----intermediaire = SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1)
                trim(SPLIT_PART(name_clean, ' ', 1) || ' ' || SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1))
            when lower(name_clean) ~ '^(saint |sainte )' then           
                trim(SPLIT_PART(name_clean, ' ', 1) || ' ' || SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1))
            when lower(name_clean) ~ '^(saint-|sainte-)' then           
                trim(SPLIT_PART(name_clean, '-', 1) || '-' || SPLIT_PART(SPLIT_PART(name_clean, '-', 2), ' ',1))                        
            ------dans tous les autres cas , on prend tout avant espace OU tiret
            else trim(SPLIT_PART(SPLIT_PART(name_clean, ' ', 1), '-',1))
        end as commune_clean,
        -------
        nom_airport,
        latitude_deg,
        longitude_deg, 
        type
    from source
)

select * from cleaned