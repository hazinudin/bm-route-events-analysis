{% macro join_satker_balai(source_alias, linkid_column = 'LINKID') %}
LEFT JOIN {{ source('misc', 'MAP_PPK_ROUTE') }} ppk
    ON {{ source_alias }}.{{ linkid_column }} = TO_CHAR(ppk.LINKID)
    AND (ppk.END_DATE IS NULL OR ppk.END_DATE > CURRENT_TIMESTAMP)
LEFT JOIN {{ source('misc', 'MAP_BALAI_ROUTE') }} br
    ON {{ source_alias }}.{{ linkid_column }} = TO_CHAR(br.LINKID)
    AND (br.END_DATE IS NULL OR br.END_DATE > CURRENT_TIMESTAMP)
LEFT JOIN {{ source('misc', 'MAP_BALAI_PROV') }} bp
    ON br.BALAI_CODE IS NULL
    AND SUBSTR({{ source_alias }}.{{ linkid_column }}, 1, 2) = bp.BM_PROV_ID
    AND (bp.END_DATE IS NULL OR bp.END_DATE > CURRENT_TIMESTAMP)
{% endmacro %}

{% macro satker_balai_columns() %}
'test' AS SATKER_PPK_ID,
99 AS BALAI_ID
{% endmacro %}
