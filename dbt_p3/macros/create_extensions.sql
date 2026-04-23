#on ajoute l'extension pour ensuite pouvoir utiliser la fonction unaccent
{% macro create_extensions() %}
    CREATE EXTENSION IF NOT EXISTS unaccent;
{% endmacro %}