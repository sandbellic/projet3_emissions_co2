
-- Requête d'analyse des émissions CO2 des voitures
SELECT

    -- Catégorisation des voitures selon leur masse
    CASE
        WHEN masse_ordma_max < 1000 THEN 'Mini'
        WHEN masse_ordma_max < 1250 THEN 'Citadine'
        WHEN masse_ordma_max < 1500 THEN 'Compacte'
        WHEN masse_ordma_max < 1750 THEN 'Berline'
        ELSE 'SUV'
    END AS categorie_masse,

    -- Catégorisation des voitures selon la puissance moteur
    CASE
        WHEN puiss_max < 70 THEN 'Faible Puissance'
        WHEN puiss_max BETWEEN 70 AND 150 THEN 'Moyenne Puissance'
        ELSE 'Haute Puissance'
    END AS type_puissance,

    -- Type d'énergie récupéré via la table de mapping
    -- (permet de transformer les codes comme ES, GO, etc.)
    m.type_energie,

    -- Nombre de voitures dans chaque groupe
    COUNT(*) AS nb_voitures,

        PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY co2_mixte::numeric) AS median_co2

FROM {{('emissions_co2.raw_cars')}} c

-- Jointure avec la table de correspondance des énergies
-- TRIM : supprime les espaces
-- UPPER : met en majuscule pour éviter les erreurs de correspondance
LEFT JOIN {{ref('energie_mapping')}} m
    ON UPPER(TRIM(c.energ)) = m.energ_raw

-- Filtre pour éviter les valeurs nulles qui faussent les calculs
WHERE co2_mixte IS NOT NULL

-- Regroupement des données par catégories
GROUP BY 
    type_puissance, 
    m.type_energie, 
    categorie_masse

-- Tri des résultats pour une meilleure lisibilité
ORDER BY 
    type_puissance, 
    m.type_energie, 
    categorie_masse
-- Calcul de la médiane des émissions de CO2
