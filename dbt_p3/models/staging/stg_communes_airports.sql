/* on va associer le n° de commune à la commune de l'aéroport */

WITH airports AS (
    SELECT *,
        trim(replace(a.commune_clean,'-' , ' ')) as commune_a_comparer
    FROM {{ ref('stg_airports') }} a
),

communes AS (
    SELECT *
    FROM {{ ref('stg_communes') }} 
),

final AS (
    SELECT
        COALESCE(c.id_commune, 0) AS id_commune,
        a.icao_code,
        a.iata_code,
        a.commune,
        a.commune_clean,
        c.commune_clean as commune_INSEE,
        nom_airport,
        a.latitude_deg,
        a.longitude_deg,
        a.type
    FROM airports a
    LEFT JOIN communes c
        ON a.commune_a_comparer = c.commune_clean
)

SELECT * FROM final