{% macro rni_pci_join(semester, year, route_selection)%}
SELECT
    b.LINKID,
    a.FROM_STA,
    a.TO_STA,
    a.SEGMENT_LENGTH,
    a.PCI,
    b.SURF_TYPE
FROM smd.pci_{{semester}}_{{year}} a
LEFT JOIN  smd.rni_{{semester}}_{{year}} b
    ON a.LINKID = b.LINKID
    AND a.FROM_STA >= b.FROM_STA
    AND a.TO_STA <= b.TO_STA
    AND a.LANE_CODE = b.LANE_CODE
WHERE PCI is not NULL {% if route_selection is not none %}AND b.LINKID in ({{"'" + route_selection | join("', '") + "'"}}){% endif %}
{% endmacro %}