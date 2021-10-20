create view ref_nomenclatures.v_nomenclatures as
(
select t.id_type,
       t.mnemonique    as type_mnemonique,
       t.label_default as type_label_default,
       n.id_nomenclature,
       n.cd_nomenclature,
       n.mnemonique    as nomenclature_mnemonique,
       n.label_default as nomenclature_label_default
from ref_nomenclatures.bib_nomenclatures_types t
         join ref_nomenclatures.t_nomenclatures n on t.id_type = n.id_type
    );


create table src_historical_data.arthropologia_tech_obs
(
    meth_obs                 varchar,
    id_nomenclature_tech_obs int references ref_nomenclatures.t_nomenclatures
);

INSERT INTO src_historical_data.arthropologia_tech_obs(meth_obs)
select lower(trim(unaccent(meth_collecte)))
from src_historical_data.arthropologia_data
group by lower(trim(unaccent(meth_collecte)))
order by lower(trim(unaccent(meth_collecte))) asc;


select *
from ref_nomenclatures.v_nomenclatures
where type_mnemonique like 'STATUT_BIO';


INSERT INTO ref_nomenclatures.bib_nomenclatures_types(mnemonique, label_default, definition_default, label_fr,
                                                      definition_fr, label_en, definition_en, source, statut)
values ('INTERACTION', 'Intéractions', 'Intéractions interspécifiques', 'Intéractions interspécifiques',
        'Intéractions interspécifiques', 'Inter-species interactions', 'Inter-species interactions', 'FLAVIA_APE',
        'Non validé');


insert into ref_nomenclatures.t_nomenclatures (id_type, cd_nomenclature, mnemonique, label_default, definition_default,
                                               label_fr, definition_fr, label_en, definition_en, label_es,
                                               definition_es, label_de, definition_de, label_it, definition_it, source,
                                               statut, id_broader, hierarchy, meta_create_date, meta_update_date,
                                               active)
values (ref_nomenclatures.get_id_nomenclature_type('INTERACTION'), '4', 'SYMBIOSE', 'Symbiose',
        'L''interaction observée est de type symbiotique, les deux taxons bénéficient de la relation (fourmis-chenilles etc)',
        'Symbiose',
        'L''interaction observée est de type symbiotique, les deux taxons bénéficient de la relation (fourmis-chenilles etc)',
        null, null, null, null, null, null, null, null, 'FLAVIA APE', 'Non validé', 0, '125.000.004',
        '2021-07-12 19:07:07.173002', null, true),
       (ref_nomenclatures.get_id_nomenclature_type('INTERACTION'), '3', 'ACC_IS', 'Accouplement interspécifique',
        'L''interaction observée est un accouplement entre deux espèces, avec ou sans reproduction résultante (hybridation)',
        'Accouplement interspécifique',
        'L''interaction observée est un accouplement entre deux espèces, avec ou sans reproduction résultante (hybridation)',
        null, null, null, null, null, null, null, null, 'FLAVIA APE', 'Non validé', 0, '125.000.003',
        '2021-07-12 19:07:07.173002', null, true),
       (ref_nomenclatures.get_id_nomenclature_type('INTERACTION'), '2', 'PARASITE', 'Parasitime',
        'L''interaction observée est de type parasitique, l''un des taxons subit le parasitisme de l''autre',
        'Parasitime',
        'L''interaction observée est de type parasitique, l''un des taxons subit le parasitisme de l''autre', null,
        null, null, null, null, null, null, null, 'FLAVIA APE', 'Non validé', 0, '125.000.002',
        '2021-07-12 19:07:07.173002', null, true),
       (ref_nomenclatures.get_id_nomenclature_type('INTERACTION'), '1', 'COMMENSALE', 'Support et déplacement',
        'L''interaction est de type commensale, l''un des taxons utilise l''autre comme support (ponte, ensoleillement, déplacement zoochorique...)',
        'Support et déplacement',
        'L''interaction est de type commensale, l''un des taxons utilise l''autre comme support (ponte, ensoleillement, déplacement zoochorique...)',
        null, null, null, null, null, null, null, null, 'FLAVIA APE', 'Non validé', 0, '125.000.001',
        '2021-07-12 19:07:07.173002', null, true),
       (ref_nomenclatures.get_id_nomenclature_type('INTERACTION'), '0', 'TROPHIQUE', 'Interaction trophique',
        'L''interaction observée est de type trophique, l''un des taxons se nourrit de l''autre : brouttage, butinage, prédation...',
        'Interaction trophique',
        'L''interaction observée est de type trophique, l''un des taxons se nourrit de l''autre : brouttage, butinage, prédation...',
        null, null, null, null, null, null, null, null, 'FLAVIA APE', 'Non validé', 0, '125.000.000',
        '2021-07-12 19:07:07.173002', null, true);

