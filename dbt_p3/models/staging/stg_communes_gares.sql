/* on va associer le n° de commune à la commune de la gare */

WITH gares AS (
    SELECT *
    FROM {{ ref('stg_gares') }} 
),

communes AS (
    SELECT *
    FROM {{ ref('stg_communes') }} 
),

final AS (
    SELECT
        COALESCE(c.id_commune, 0) as id_commune,
        g.code_uic,
        g.commune,
        g.commune_clean,
        g.nom_gare,
        g.latitude_deg,
        g.longitude_deg
    FROM  gares as g
    LEFT JOIN communes c
        ON g.commune_clean = lower(c.commune)
)

SELECT * FROM final