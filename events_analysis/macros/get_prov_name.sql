{% macro get_prov_name(source_alias, linkid_column = 'LINKID') %}
LEFT JOIN {{ source('misc', 'MAP_PROV_ROUTE') }} mpr
    ON {{ source_alias }}.{{ linkid_column }} = mpr.LINKID
    AND mpr.END_DATE IS NULL
LEFT JOIN {{ source('misc', 'REF_PROVINCE') }} rp
    ON SUBSTR({{ source_alias }}.{{ linkid_column }}, 1, 2) = rp.BM_PROV_ID
{% endmacro %}

{% macro prov_name_column() %}
COALESCE(mpr.PROV_NAME, rp.PROV_NAME)
{% endmacro %}
