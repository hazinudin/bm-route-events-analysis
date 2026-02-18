{% macro rni_iri_join(semester, year, route_selection)%}
SELECT
    b.LINKID,
    b.FROM_STA,
    b.TO_STA,
    b.SEGMENT_LENGTH,
    a.IRI,
    a.IRI_POK,
    b.SURF_TYPE
FROM (select * from {{ref("stg_rni_combined")}} where year = {{year}} and semester = {{semester}}) b
LEFT JOIN  smd.roughness_{{semester}}_{{year}} a
    ON a.LINKID = b.LINKID
    AND a.FROM_STA = b.FROM_STA
    AND a.LANE_CODE = b.LANE_CODE
WHERE IRI is not NULL {% if route_selection is not none %}AND b.LINKID in ({{"'" + route_selection | join("', '") + "'"}}){% endif %}
{% endmacro %}