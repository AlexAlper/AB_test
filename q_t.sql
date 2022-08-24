SELECT toUInt8OrZero(JSON_VALUE(body, '$.source'))    AS source,
       upper(toString(JSON_VALUE(body, '$.number')))  AS number, -- 1 каунт дистинкг
       lower(JSON_VALUE(body, '$.id_element'))        AS id_element,
       toUInt64OrNull(JSON_VALUE(body, '$.id_order')) AS id_cart,
       toUInt64OrNull(JSON_VALUE(body, '$.id_tov'))   AS id_tov
FROM raw.logs_tbl
WHERE _date_start > '2022-08-01'
    AND source IN (2, 3, 4, 5)
    AND proc_name = 'universal_log_insert'
GROUP BY source, number;