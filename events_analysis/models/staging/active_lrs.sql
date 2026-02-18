{{
    config(
        materialized='view'
    )
}}

SELECT LINKID, SK_LENGTH from {{source('lrs', 'road_network_nat')}}
WHERE
(fromdate is null or fromdate < current_timestamp) and (todate is null or todate > current_timestamp)