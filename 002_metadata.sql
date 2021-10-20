-- truncate gn_meta.t_acquisition_frameworks restart identity cascade;/**/
-- ROLLBACK;
BEGIN;

/* Cadre d'acquisition manuellement créé via GeoNature > Métadonnées */


-- insert into gn_meta.t_acquisition_frameworks (acquisition_framework_name, acquisition_framework_desc, territory_desc,
--                                               is_parent, id_digitizer,
--                                               acquisition_framework_start_date, acquisition_framework_end_date,
--                                               meta_create_date, meta_update_date, initial_closing_date)
-- SELECT 'A Classer',
--        'JDD à classer',
--        'Auvergne-Rhône-Alpes',
--        FALSE,
--        id_role,
--        min(date_min),
--        max(date_max),
--        now(),
--        now(),
--        null
-- from utilisateurs.t_roles,
--      src_historical_data.arthropologia_data
-- where identifiant = 'admin'
-- group by id_role;

insert into gn_meta.t_datasets (id_acquisition_framework, dataset_name, dataset_shortname, dataset_desc, keywords,
                                marine_domain, terrestrial_domain, id_digitizer, id_taxa_list, meta_create_date,
                                meta_update_date)
select id_acquisition_framework,
       nom_jdd,
       nom_jdd,
       'A Compléter',
       null,
       False,
       True,
       id_role,
       null,
       now(),
       now()
from gn_meta.t_acquisition_frameworks,
     src_historical_data.arthropologia_data,
     utilisateurs.t_roles
where acquisition_framework_name = 'A Classer'
  and identifiant = 'admin'
group by nom_jdd, id_acquisition_framework, id_role;

/* Organisme Arthropologia créé avec un id_organisme = 2 */

/* Acteurs du JDD (Arthropologia en acteur principal) */

INSERT INTO gn_meta.cor_dataset_actor(id_dataset, id_organism, id_nomenclature_actor_role)
select id_dataset, 2, ref_nomenclatures.get_id_nomenclature('ROLE_ACTEUR', '1')
from gn_meta.t_datasets
on conflict (id_dataset,id_organism, id_nomenclature_actor_role) DO NOTHING;


/* Correspondance JDD Territoire (Métropole) */

INSERT INTO gn_meta.cor_dataset_territory(id_dataset, id_nomenclature_territory)
select id_dataset, ref_nomenclatures.get_id_nomenclature('TERRITOIRE', 'METROP')
from gn_meta.t_datasets
on conflict (id_dataset,id_nomenclature_territory) DO NOTHING;

INSERT INTO gn_commons.cor_module_dataset(id_module, id_dataset)
select id_module, id_dataset
from gn_commons.t_modules,
     gn_meta.t_datasets
where module_code like 'OCCTAX'
ON CONFLICT (id_module, id_dataset) DO NOTHING;
COMMIT;
