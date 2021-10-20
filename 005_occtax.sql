ROLLBACK;
BEGIN;

truncate pr_occtax.t_releves_occtax restart identity cascade;

WITH t1 as (select distinct row_number() over (partition by uuid_releve)                as rank,
                            uuid_releve                                                 as unique_id_sinp_grp,
                            id_dataset                                                  as id_dataset,
                            2                                                           as id_digitiser,
                            null                                                        as observers_txt,
                            teco.id_nomenclature_tech_obs                               as id_nomenclature_tech_collect_campanule,
                            ref_nomenclatures.get_id_nomenclature('TYP_GRP', 'NSP')     as id_nomenclature_grp_typ,
                            null                                                        as grp_method,
                            MIN(date_min)                                               as date_min,
                            MAX(date_max)                                               as date_max,
                            MIN(heure_min)                                              as hour_min,
                            MAX(case
                                    when heure_max = '00:00:00'::time then '23:59:00'::time
                                    else heure_max end)                                 as hour_max,
                            site || ' ' || station                                      as place_name,
                            null                                                        as meta_device_entry,
                            'Import historique Arthropologia'                           as comment,
                            st_transform(geom, 2154)                                    as geom_local,
                            st_transform(geom, 4326)                                    as geom_4326,
                            ref_nomenclatures.get_id_nomenclature('NAT_OBJ_GEO', 'NSP') as id_nomenclature_geo_object_nature,
                            precision                                                   as precision,
                            jsonb_build_object('weather', ciel)                         as additional_fields
            from src_historical_data.arthropologia_data
                     join gn_meta.t_datasets on nom_jdd = dataset_name
                     left join src_historical_data.arthropologia_tech_obs as teco
                               on teco.meth_obs = lower(trim(unaccent(arthropologia_data.meth_collecte)))
            where importable
            group by uuid_releve, precision, id_dataset, teco.id_nomenclature_tech_obs, ciel, site || ' ' || station,
                     geom)
INSERT
INTO pr_occtax.t_releves_occtax(unique_id_sinp_grp, id_dataset, id_digitiser, observers_txt,
                                id_nomenclature_tech_collect_campanule,
                                id_nomenclature_grp_typ, grp_method, date_min, date_max, hour_min, hour_max,
                                place_name, meta_device_entry, comment,
                                geom_local, geom_4326, id_nomenclature_geo_object_nature, precision,
                                additional_fields)
select distinct unique_id_sinp_grp,
                id_dataset,
                id_digitiser,
                observers_txt,
                id_nomenclature_tech_collect_campanule,
                id_nomenclature_grp_typ,
                grp_method,
                date_min,
                date_max,
                hour_min,
                hour_max,
                place_name,
                meta_device_entry,
                comment,
                geom_local,
                geom_4326,
                id_nomenclature_geo_object_nature,
                precision,
                additional_fields
from t1
where rank = 1
;
COMMIT;

/* Ajout des données Ciel aux relevés */
-- ROLLBACK;
-- BEGIN;
-- UPDATE pr_occtax.t_releves_occtax
-- set additional_fields = '{}'::jsonb;
--
-- WITH t1 as (select distinct uuid_releve, ciel as weather, rank() over (partition by uuid_releve) as rank
--             from src_historical_data.arthropologia_data
--             where ciel is not null)
-- update pr_occtax.t_releves_occtax
-- set additional_fields = jsonb_set(additional_fields, '{0,weather}', to_jsonb(weather))
-- from t1
-- where uuid_releve = unique_id_sinp_grp
--   and rank = 1;
--
-- COMMIT;

select *
from pr_occtax.t_releves_occtax
where unique_id_sinp_grp = '32c83c82-af1d-4d29-9b59-42b84957dcc8';

ROLLBACK;
BEGIN;

INSERT INTO pr_occtax.t_occurrences_occtax(id_releve_occtax, unique_id_occurence_occtax, id_nomenclature_obs_technique,
                                           id_nomenclature_bio_condition,
                                           id_nomenclature_bio_status,
                                           id_nomenclature_naturalness, id_nomenclature_exist_proof,
                                           id_nomenclature_diffusion_level, id_nomenclature_observation_status,
                                           id_nomenclature_blurring, id_nomenclature_source_status,
                                           id_nomenclature_behaviour, determiner, id_nomenclature_determination_method,
                                           meta_v_taxref, cd_nom, nom_cite
    , sample_number_proof, digital_proof, non_digital_proof,
                                           comment, additional_fields)
select distinct t_releves_occtax.id_releve_occtax
              , uuid_occurence
              , ref_nomenclatures.get_id_nomenclature('METH_OBS', '0')
              , case
                    when detail_collecte in ('barber',
                                             'mort sur chemin de forêt',
                                             'mort',
                                             'elytres'
                        )
                        then ref_nomenclatures.get_id_nomenclature('ETA_BIO', '3')
                    when detail_collecte in ('battage au ras du sol',
                                             'au sol',
                                             'en vol sur végétation en lisière de forêt',
                                             'vue',
                                             'en vol',
                                             'piège lumineux',
                                             'chant',
                                             'fauchage'
                        ) then ref_nomenclatures.get_id_nomenclature('ETA_BIO', '2')
                    else ref_nomenclatures.get_id_nomenclature('ETA_BIO', '0') end
              , pr_occtax.get_default_nomenclature_value('STATUT_BIO')
              , pr_occtax.get_default_nomenclature_value('NATURALITE')
              , case
                    when ref_echantillon is not null then ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '1')
                    else
                        ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '2') end
              , ref_nomenclatures.get_id_nomenclature('NIV_PRECIS', '0')
              , ref_nomenclatures.get_id_nomenclature('STATUT_OBS', 'Pr')
              , ref_nomenclatures.get_id_nomenclature('DEE_FLOU', 'NSP')
              , ref_nomenclatures.get_id_nomenclature('STATUT_SOURCE', 'NSP')
              , ref_nomenclatures.get_id_nomenclature('OCC_COMPORTEMENT', '1')
              , trim(replace(determinateur_final, '_', ' '))
              , ref_nomenclatures.get_id_nomenclature('METH_DETERMIN', '1')
              , gn_commons.get_default_parameter('taxref_version')
              , arthropologia_data.cd_ref
              , coalesce(arthropologia_data.taxon_valide, taxref.nom_valide)
              , ref_echantillon
              , NULL
              , string_agg(distinct num_identification, ';')
              , 'Import historique Arthropologia'
              , jsonb_build_object('source', 'Tableur excel Arthropologia', 'num_identification',
                                   to_jsonb(array_agg(distinct num_identification)))
from src_historical_data.arthropologia_data
         join pr_occtax.t_releves_occtax
              on uuid_releve = unique_id_sinp_grp
         left join taxonomie.taxref on arthropologia_data.cd_ref = taxref.cd_nom
group by t_releves_occtax.id_releve_occtax, uuid_occurence, ref_nomenclatures.get_id_nomenclature('METH_OBS', '0'),
         case
             when detail_collecte in ('barber',
                                      'mort sur chemin de forêt',
                                      'mort',
                                      'elytres'
                 )
                 then ref_nomenclatures.get_id_nomenclature('ETA_BIO', '3')
             when detail_collecte in ('battage au ras du sol',
                                      'au sol',
                                      'en vol sur végétation en lisière de forêt',
                                      'vue',
                                      'en vol',
                                      'piège lumineux',
                                      'chant',
                                      'fauchage'
                 ) then ref_nomenclatures.get_id_nomenclature('ETA_BIO', '2')
             else ref_nomenclatures.get_id_nomenclature('ETA_BIO', '0') end,
         pr_occtax.get_default_nomenclature_value('STATUT_BIO'), pr_occtax.get_default_nomenclature_value('NATURALITE'),
         case
             when ref_echantillon is not null then ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '1')
             else
                 ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '2') end,
         trim(replace(determinateur_final, '_', ' ')), arthropologia_data.cd_ref,
         coalesce(arthropologia_data.taxon_valide, taxref.nom_valide), ref_echantillon;

COMMIT;

/* Dénombrements */
ROLLBACK;
BEGIN;
-- TRUNCATE pr_occtax.cor_counting_occtax RESTART IDENTITY CASCADE;
INSERT INTO pr_occtax.cor_counting_occtax(id_occurrence_occtax, unique_id_sinp_occtax, id_nomenclature_life_stage,
                                          id_nomenclature_sex,
                                          id_nomenclature_obj_count, id_nomenclature_type_count, count_min, count_max,
                                          additional_fields)
select distinct id_occurrence_occtax,
                uuid_counting,
                ref_nomenclatures.get_id_nomenclature('STADE_VIE', case
                                                                       when stade = 'Larve' then '6'
                                                                       when stade = 'Imago' then '15'
                                                                       when stade = 'Nymphe' then '13' end),
                ref_nomenclatures.get_id_nomenclature('SEXE', case
                                                                  when trim(lower(sexe)) in ('m', 'male') then '3'
                                                                  when trim(lower(sexe)) in ('f', 'femelle', 'reine', 'ouvriere', 'o')
                                                                      then '2'
                                                                  else '6' end),
                ref_nomenclatures.get_id_nomenclature('OBJ_DENBR', case
                                                                       when detail_collecte in ('battage au ras du sol',
                                                                                                'au sol',
                                                                                                'en vol sur végétation en lisière de forêt',
                                                                                                'vue',
                                                                                                'en vol',
                                                                                                'piège lumineux',
                                                                                                'fauchage',
                                                                                                'barber',
                                                                                                'mort sur chemin de forêt',
                                                                                                'mort',
                                                                                                'elytres') then 'IND'
                                                                       else 'NSP'
                    end),
                ref_nomenclatures.get_id_nomenclature('TYP_DENBR', 'NSP'),
                replace(abondance, '>=', '')::int,
                replace(abondance, '>=', '')::int,
                jsonb_build_object('num_identification', num_identification, 'source_data',
                                   row_to_json(arthropologia_data))
from src_historical_data.arthropologia_data
         join pr_occtax.t_occurrences_occtax on uuid_occurence = t_occurrences_occtax.unique_id_occurence_occtax
ON CONFLICT DO NOTHING;

COMMIT;


BEGIN;
with t1 as (select distinct trim(unnest(string_to_array(lower(nom_prenom), '/'))) as nom_prenom, uuid_releve
            from src_historical_data.arthropologia_data)

   , t2 as (select distinct id_releve_occtax, unaccent(nom_prenom) as identifiant, id_role
            from t1
                     join utilisateurs.t_roles on unaccent(t1.nom_prenom) = t_roles.identifiant
                     join pr_occtax.t_releves_occtax on uuid_releve = unique_id_sinp_grp)
INSERT
INTO pr_occtax.cor_role_releves_occtax(id_releve_occtax, id_role)
select id_releve_occtax, id_role
from t2
where id_role != 37;
COMMIT;

-- delete
-- from pr_occtax.cor_role_releves_occtax
-- where id_role = 37;
-- ROLLBACK;

update pr_occtax.t_releves_occtax
set additional_fields = jsonb_set(additional_fields, '{trapping_type}', to_jsonb(type_piegeage))
from src_historical_data.arthropologia_data
where uuid_releve = unique_id_sinp_grp;

update pr_occtax.t_releves_occtax
set additional_fields = jsonb_set(additional_fields, '{site}', to_jsonb(site))
from src_historical_data.arthropologia_data
where uuid_releve = unique_id_sinp_grp;

update pr_occtax.t_releves_occtax
set additional_fields = jsonb_set(additional_fields, '{station}', to_jsonb(station))
from src_historical_data.arthropologia_data
where uuid_releve = unique_id_sinp_grp;


update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_set(additional_fields, '{individual_location}', to_jsonb(emplacement_individu))
from src_historical_data.arthropologia_data
where uuid_counting = unique_id_sinp_occtax;

update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_set(additional_fields, '{caste}', to_jsonb(caste))
from src_historical_data.arthropologia_data
where uuid_counting = unique_id_sinp_occtax;

update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_set(additional_fields, '{sampling_method}', to_jsonb(meth_collecte))
from src_historical_data.arthropologia_data
where uuid_counting = unique_id_sinp_occtax;

update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_set(additional_fields, '{barber_distance}', to_jsonb(dist_barber))
from src_historical_data.arthropologia_data
where uuid_counting = unique_id_sinp_occtax;

update pr_occtax.t_releves_occtax
set additional_fields = jsonb_set(additional_fields, '{station}', to_jsonb(station))
from src_historical_data.arthropologia_data
where uuid_releve = unique_id_sinp_grp;

select distinct t_releves_occtax.*
from pr_occtax.t_releves_occtax
         join pr_occtax.t_occurrences_occtax
              on t_releves_occtax.id_releve_occtax = t_occurrences_occtax.id_releve_occtax
         join pr_occtax.cor_counting_occtax
              on t_occurrences_occtax.id_occurrence_occtax = cor_counting_occtax.id_occurrence_occtax
where cor_counting_occtax.additional_fields #>> '{individual_location}' = 'En vol'
limit 10;

-- select distinct jsonb_object_keys(cor_counting_occtax.additional_fields)
-- from pr_occtax.cor_counting_occtax;
--

update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_build_object('num_identification', num_identification, 'individual_location',
                                           emplacement_individu, 'caste', caste, 'sampling_method', meth_collecte,
                                           'barber_distance', dist_barber, 'source_data',
                                           row_to_json(arthropologia_data))
from src_historical_data.arthropologia_data
where uuid_counting = unique_id_sinp_occtax;

update src_historical_data.arthropologia_data
set etat_bio = 'Trouvé mort'
where etat_bio = 'Trouvé Mort';

with t1 as (select distinct uuid_occurence, (array_agg(arthropologia_data.etat_bio))[1] as eta_bio
            from src_historical_data.arthropologia_data
                     join src_historical_data.tmp_arthropologia_data_updates
                          on arthropologia_data.num_identification = tmp_arthropologia_data_updates.num_dentification
            group by uuid_occurence)
        ,
     t2 as (
         select uuid_occurence, eta_bio, id_nomenclature, cd_nomenclature, nomenclature_label_default
         from t1
                  left join (select id_nomenclature, cd_nomenclature, nomenclature_label_default
                             from ref_nomenclatures.v_nomenclatures
                             where type_mnemonique = 'ETA_BIO') t on nomenclature_label_default = eta_bio
         /*and tech_obs is not null*/)
update pr_occtax.t_occurrences_occtax
set id_nomenclature_bio_condition = coalesce(id_nomenclature, ref_nomenclatures.get_id_nomenclature('ETA_BIO', '1'))
from t2
where uuid_occurence = unique_id_occurence_occtax;


with t1 as (select distinct uuid_occurence, (array_agg(arthropologia_data.tech_obs))[1] as eta_bio
            from src_historical_data.arthropologia_data
                     join src_historical_data.tmp_arthropologia_data_updates
                          on arthropologia_data.num_identification = tmp_arthropologia_data_updates.num_dentification
            group by uuid_occurence)
        ,
     t2 as (
         select uuid_occurence, eta_bio, id_nomenclature, cd_nomenclature, nomenclature_label_default
         from t1
                  left join (select id_nomenclature, cd_nomenclature, nomenclature_label_default
                             from ref_nomenclatures.v_nomenclatures
                             where type_mnemonique = 'METH_OBS') t on nomenclature_label_default = eta_bio
         /*and tech_obs is not null*/)
update pr_occtax.t_occurrences_occtax
set id_nomenclature_obs_technique= coalesce(id_nomenclature, ref_nomenclatures.get_id_nomenclature('METH_OBS', '21'))
from t2
where uuid_occurence = unique_id_occurence_occtax;

insert into taxonomie.bib_noms (cd_nom, cd_ref, nom_francais, comments)
select distinct synthese.cd_nom,
                taxref.cd_ref,
                split_part(nom_vern, ',', 1),
                'Ajouté depuis les données historiques'
from gn_synthese.synthese
         join taxonomie.taxref on synthese.cd_nom = taxref.cd_nom;

insert into taxonomie.cor_nom_liste(id_liste, id_nom)
select 100, id_nom
from taxonomie.bib_noms;


select populate_geometry_columns();

select t_releves_occtax.id_releve_occtax,
       t_releves_occtax.additional_fields,
       string_agg(distinct cor_counting_occtax.additional_fields #>>
                           '{source_data,ciel}', ', ')
from pr_occtax.t_releves_occtax
         join pr_occtax.t_occurrences_occtax
              on t_releves_occtax.id_releve_occtax = t_occurrences_occtax.id_releve_occtax
         join pr_occtax.cor_counting_occtax
              on t_occurrences_occtax.id_occurrence_occtax = cor_counting_occtax.id_occurrence_occtax
where (cor_counting_occtax.additional_fields #>> '{source_data,ciel}') is not null
group by t_releves_occtax.id_releve_occtax, t_releves_occtax.additional_fields
;
with t1 as (
    select t_releves_occtax.id_releve_occtax,
--        t_releves_occtax.additional_fields,
           case
               when t_releves_occtax.additional_fields is null and
                    string_agg(distinct cor_counting_occtax.additional_fields #>>
                                        '{source_data,ciel}', ', ') is not null then
                   jsonb_build_object('weather', to_jsonb(string_agg(distinct cor_counting_occtax.additional_fields #>>
                                                                              '{source_data,ciel}', ', ')))
               when string_agg(distinct cor_counting_occtax.additional_fields #>> '{source_data,ciel}',
                               ', ') is not null
                   then jsonb_set(t_releves_occtax.additional_fields, '{weather}'::TEXT[],
                                  to_jsonb(string_agg(distinct
                                                      cor_counting_occtax.additional_fields #>> '{source_data,ciel}',
                                                      ', ')), true)
               else t_releves_occtax.additional_fields end as new_additional_fields
--        to_json(string_agg(distinct cor_counting_occtax.additional_fields #>> '{source_data,ciel}', ', ')),
--        t_releves_occtax.additional_fields is not null and string_agg(distinct cor_counting_occtax.additional_fields #>>
--                                                                           '{source_data,ciel}', ', ') is not null
    from pr_occtax.t_releves_occtax
             join pr_occtax.t_occurrences_occtax
                  on t_releves_occtax.id_releve_occtax = t_occurrences_occtax.id_releve_occtax
             JOIN pr_occtax.cor_counting_occtax
                  on t_occurrences_occtax.id_occurrence_occtax = cor_counting_occtax.id_occurrence_occtax

    group by t_releves_occtax.id_releve_occtax, t_releves_occtax.additional_fields
    having string_agg(distinct
                      cor_counting_occtax.additional_fields #>> '{source_data,ciel}',
                      ', ') is not null
)
update pr_occtax.t_releves_occtax
set additional_fields = new_additional_fields
from t1
where t1.id_releve_occtax = t_releves_occtax.id_releve_occtax;

update pr_occtax.cor_counting_occtax
set additional_fields = jsonb_set(cor_counting_occtax.additional_fields, '{plante}'::text[],
                                  cor_counting_occtax.additional_fields #> '{source_data,hote_famille}', true)
where not additional_fields ? 'plante';

with t1 as (select arthropologia_data.uuid_occurence, string_agg(distinct rq, ' | ') as rqs
            from src_historical_data.arthropologia_data
            group by uuid_occurence)
update pr_occtax.t_occurrences_occtax
set comment = concat(comment, ' | ',rqs)
from t1
where uuid_occurence = unique_id_occurence_occtax
  and rqs is not null;

select comment from pr_occtax.t_occurrences_occtax;