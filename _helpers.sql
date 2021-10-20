
create or replace function ref_geo.fct_c_farthest_node_from_centroid_distance(_geom geometry, _default integer default 0)
    RETURNS float AS
$dist$
DECLARE
    _dist float;
BEGIN
    RAISE NOTICE 'Geometry type is %', st_geometrytype(_geom);
    if st_geometrytype(_geom) like 'ST_Point'
    then
        RAISE NOTICE 'Geometry type is "ST_Point"';
        select _default into _dist;
    else
        RAISE NOTICE 'Calculating distance';
        with
            t as (select (st_dumppoints(_geom)).geom)
        select
            max(st_distance(st_centroid(_geom), t.geom))
            into _dist
            from
                t;
        RAISE NOTICE 'dist is %', _dist;
    end if;
    return _dist;
END;
$dist$
    LANGUAGE plpgsql;
