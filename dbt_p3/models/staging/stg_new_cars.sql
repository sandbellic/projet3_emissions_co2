
-- Requête d'analyse des émissions CO2 des voitures

SELECT     -- Type d'énergie récupéré via la table de mapping
    -- (permet de transformer les codes comme ES, GO, etc.)
    -- Nombre de voitures dans chaque groupe
    carrosserie, energie, 
    COUNT(*) AS nb_voitures,

        PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY co2_moyen::numeric) AS co2_median

FROM {{ref('stg_liste_new_cars')}} as lnc

-- Jointure avec la table de correspondance des énergies
-- TRIM : supprime les espaces
-- UPPER : met en majuscule pour éviter les erreurs de correspondance
LEFT JOIN {{ref('energie_mapping')}} m
    ON UPPER(TRIM(lnc.energie)) = m.energ_raw

-- Regroupement des données par catégories
GROUP BY 
    lnc.energie, 
    lnc.carrosserie


-- Tri des résultats pour une meilleure lisibilité
ORDER BY 
    lnc.energie, 
    lnc.carrosserie
-- Calcul de la médiane des émissions de CO2