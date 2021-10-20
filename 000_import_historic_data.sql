BEGIN;
CREATE SCHEMA IF NOT EXISTS src_historical_data;

DROP TABLE IF EXISTS src_historical_data.arthropologia_data;
CREATE TABLE src_historical_data.arthropologia_data
(
    code_releve         VARCHAR(50),
    num_identification  VARCHAR(50),
    pays                VARCHAR(50),
    dept                VARCHAR(50),
    site                VARCHAR(250),
    station             VARCHAR(250),
    commune             VARCHAR(250),
    lieudit             VARCHAR(500),
    latitude            FLOAT,
    longitude           FLOAT,
    altitude            VARCHAR,
    milieu              VARCHAR(500),
    date                VARCHAR(50),
    date_min            DATE,
    date_max            DATE,
    date_heure_min      TIMESTAMP,
    date_heure_max      TIMESTAMP,
    nom_prenom          VARCHAR(250),
    ciel                VARCHAR(250),
    temperature         VARCHAR(250),
    heure               VARCHAR(50),
    heure_min           TIME,
    heure_max           TIME,
    meth_collecte       VARCHAR(250),
    detail_collecte     VARCHAR(250),
    couleur_dist_barber VARCHAR(250),
    hote_famille        VARCHAR(250),
    hote_espece         VARCHAR(250),
    hote_determinateur  VARCHAR(250),
    ordre               VARCHAR(250),
    famille             VARCHAR(250),
    taxon_valide        VARCHAR(250),
    cd_ref              INT,
    subsp               VARCHAR(250),
    sexe                VARCHAR(10),
    stade               VARCHAR(250),
    predeterm_ordre     VARCHAR(250),
    predeterm_determ    VARCHAR(250),
    predeterm_an        INT,
    predeterm_taxon     VARCHAR(250),
    determinateur_final VARCHAR(250),
    annee_determination INT,
    taxon_deternine     VARCHAR(250),
    rq                  TEXT,
    typ_stockage        VARCHAR(250),
    ref_echantillon     VARCHAR(250),
    abondance           VARCHAR(20),
    numerisation        VARCHAR(250),
    nom_jdd             VARCHAR(500),
    geom                GEOMETRY(point, 2154) generated always as (
                            st_transform(
                                    st_setsrid(
                                            st_makepoint(longitude, latitude), 4326)
                                , 2154)
                            ) STORED
);


CREATE INDEX ON src_historical_data.arthropologia_data USING gist (geom);

ALTER TABLE src_historical_data.arthropologia_data
    ADD COLUMN uuid_releve UUID;
ALTER TABLE src_historical_data.arthropologia_data
    ADD COLUMN uuid_occurence UUID;
ALTER TABLE src_historical_data.arthropologia_data
    ADD COLUMN uuid_counting UUID;
ALTER TABLE src_historical_data.arthropologia_data
    ADD COLUMN precision INT;
ALTER TABLE src_historical_data.arthropologia_data
    add column importable boolean default false;


/* Ajout des centroid "commune"+precision lorsque pas de coordonnées */
UPDATE src_historical_data.arthropologia_data
SET longitude = st_x(st_transform(st_centroid(la.geom), 4326)),
    latitude  = st_y(st_transform(st_centroid(la.geom), 4326)),
    precision = ref_geo.fct_c_farthest_node_from_centroid_distance(la.geom)::int
from ref_geo.l_areas la
where arthropologia_data.geom is null
  and lower(unaccent(la.area_name)) = lower(unaccent(commune));

/* Marquage des données non importables lorsque pas de coordonnées */

UPDATE src_historical_data.arthropologia_data
set importable = (latitude is NOT null and longitude is NOT null);

/* Generate Releve UUID */


WITH t1 as (select distinct code_releve, nom_jdd, geom, date_min, date_max
            from src_historical_data.arthropologia_data
--             where uuid_releve is null
            group by code_releve, nom_jdd, geom, date_min, date_max)
   , t2 as (
    select code_releve, nom_jdd, geom, date_min, date_max, uuid_generate_v4() as uuid_releve
    from t1
)
UPDATE src_historical_data.arthropologia_data
set uuid_releve = t2.uuid_releve
from t2
where (arthropologia_data.code_releve, arthropologia_data.nom_jdd, arthropologia_data.geom, arthropologia_data.date_min,
       arthropologia_data.date_max) = (t2.code_releve, t2.nom_jdd, t2.geom, t2.date_min, t2.date_max)
  and arthropologia_data.uuid_releve is null;

/* Generate Occurence UUID */

WITH t1 as (select uuid_releve, cd_ref from src_historical_data.arthropologia_data where uuid_occurence is null)
   , t2 as (select uuid_releve, cd_ref, uuid_generate_v4() as uuid_occurence from t1)
UPDATE src_historical_data.arthropologia_data
set uuid_occurence = t2.uuid_occurence
from t2
where (arthropologia_data.uuid_releve, arthropologia_data.cd_ref) = (t2.uuid_releve, t2.cd_ref);

/* Generate Counting UUID */
update src_historical_data.arthropologia_data
set uuid_counting = uuid_generate_v4()
where uuid_counting is null;


/* Données mises à jour par Arthropologia depuis l'import */
COMMIT;

create unique index on src_historical_data.arthropologia_data (num_identification);
select *
from src_historical_data.arthropologia_data
where num_identification = '2020.04082';

with t1 as (
    select num_identification, count(*) from src_historical_data.arthropologia_data group by num_identification)
select arthropologia_data.*
from t1
         join src_historical_data.arthropologia_data on t1.num_identification = arthropologia_data.num_identification
where t1.count > 1;

create table src_historical_data.tmp_arthropologia_data_updates
(
    num_dentification    varchar,
    method_collect       varchar,
    type_piegeage        varchar,
    emplacement_individu varchar,
    tech_obs             varchar,
    etat_bio             varchar,
    dist_barber          varchar,
    sex                  varchar,
    caste                varchar,
    stade                varchar,
    comment              text
);

alter table src_historical_data.arthropologia_data
    add column caste varchar;

alter table src_historical_data.arthropologia_data
    add column tech_obs varchar;

alter table src_historical_data.arthropologia_data
    add column etat_bio varchar;

alter table src_historical_data.arthropologia_data
    add column emplacement_individu varchar;


alter table src_historical_data.arthropologia_data
    add column dist_barber varchar;

alter table src_historical_data.arthropologia_data
    add column type_piegeage varchar;


update src_historical_data.arthropologia_data
set meth_collecte        = t.method_collect,
    type_piegeage        = t.type_piegeage,
    emplacement_individu = t.emplacement_individu,
    tech_obs             = t.tech_obs,
    etat_bio             = t.etat_bio,
    dist_barber          = t.dist_barber,
    sexe                 = t.sex,
    caste                = t.caste,
    stade                = t.stade,
    rq                   = t.comment
from src_historical_data.tmp_arthropologia_data_updates as t
where arthropologia_data.num_identification = t.num_dentification;


