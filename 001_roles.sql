/* Création des roles utilisateurs à partir du champ nom_prenom */

-- with names as (select distinct trim(unnest(string_to_array(lower(nom_prenom), '/'))) as nom_prenom
--                from src_historical_data.arthropologia_data)
-- select distinct unaccent(nom_prenom),
--                 unaccent(upper(substring(nom_prenom from '^[^_]+(?=_)'))) AS nom,
--                 substring(nom_prenom from '[^_]*_(.*)')                   as prenom
-- from names;

BEGIN;
with t1 as (select distinct trim(unnest(string_to_array(lower(nom_prenom), '/'))) as nom_prenom
            from src_historical_data.arthropologia_data)

   , t2 as (select distinct unaccent(nom_prenom)                                      as identifiant,
                            unaccent(upper(substring(nom_prenom from '^[^_]+(?=_)'))) AS nom,
                            substring(nom_prenom from '[^_]*_(.*)')                   as prenom
            from t1)
INSERT
INTO utilisateurs.t_roles (identifiant, nom_role, prenom_role, champs_addi, remarques, date_insert, date_update)
select identifiant,
       nom,
       prenom,
       json_build_object('source', 'Arthropologia'),
       'Source: Arthropologia',
       now(),
       now()
from t2;
COMMIT;