-- SELECT toUInt8OrZero(JSON_VALUE(body, '$.source'))    AS source,
--        upper(toString(JSON_VALUE(body, '$.number')))  AS number, -- 1 каунт дистинкг
--        lower(JSON_VALUE(body, '$.id_element'))        AS id_element,
--        toUInt64OrNull(JSON_VALUE(body, '$.id_order')) AS id_cart,
--        toUInt64OrNull(JSON_VALUE(body, '$.id_tov'))   AS id_tov
-- FROM raw.logs_tbl
-- WHERE _date_start > '2022-08-01'
--     AND source IN (2, 3, 4, 5)
--     AND proc_name = 'universal_log_insert'
-- GROUP BY source, number;

[
    {"metrics": [
        "Количество клиентов, посещавших МП"
    ],
    "query": "
        SELECT COUNT(DISTINCT number) AS number,
        FROM raw.logs_tbl
        WHERE date_add between ''{date_1}'' and ''{date_2}'';
    "},
    {"metrics": [
        "MS Количество клиентов, оформивших заказ"
    ],
    "query": "
        SELECT COUNT(DISTINCT number) as number
        FROM Loyalty03..Orders_header WITH (NOLOCK)
        WHERE date_order between DATEFROMPARTS({year_1}, {month_1}, {day_1}) and DATEFROMPARTS({year_2}, {month_2}, {day_2})
            AND source IN (2, 3, 4, 5)
            AND id_status in (5, 9, 18)
            AND order_type in (2, 4);
    "},
    {"metrics": [
        "Количество уникальных корзин за период"
    ],
    "query": "
        SELECT COUNT(DISTINCT id_cart) as id_cart
        FROM raw.logs_tbl
        WHERE date_add between ''{date_1}'' and ''{date_2}'';
    "},
    {"metrics": [
        "Количество добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)"
    ],
    "query": "
        SELECT COUNT(DISTINCT id_tov) as id_tov
        FROM raw.logs_tbl
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND isNotNull(id_tov)
            AND id_element IN (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "MS Выручка добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)"
    ],
    "query": "
        ms нужны заказы
    "},
    {"metrics": [
        "MS Маржа добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)"
    ],
    "query": "
        ms нужны заказы
    "},
    {"metrics": [
        "Количество клиентов, заходивших в поиск"
    ],
    "query": "
        SELECT COUNT(DISTINCT number)   as number
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list};
    "},
    {"metrics": [
        "Количество клиентов, добавлявших товары из непустого поиска"
    ],
    "query": "
        SELECT COUNT(DISTINCT number)    as number
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND search_bar != ''
            AND id_element IN (
                                ''add'',
                                ''plus'',
                                ''minus'',
                                ''edit''
            );
    "},
    {"metrics": [
        "Количество уникальных выдач (не пустых)"
    ],
    "query": "
        SELECT COUNT(DISTINCT id_search) as id_search
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND search_bar != '';
    "},
    {"metrics": [
        "Количество разных запросов"
    ],
    "query": "
        SELECT COUNT(DISTINCT search_bar) as search_bar
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list};
    "},
    {"metrics": [
        "Количество уникальных выдач с добавлением (не пустых)"
    ],
    "query": "
        SELECT COUNT(DISTINCT id_search) as id_search
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            )
            AND search_bar != '';
    "},
    {"metrics": [
        "Количество товаров добавленных из поиска (не пустого)"
    ],
    "query": "
        SELECT COUNT(id_tov) as id_tov
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            )
            AND search_bar != '';
    "},
    {"metrics": [
        "MS Выручка товаров добавленных из поиска (не пустого)"
    ],
    "query": "
        ms нужны заказы
    "},
    {"metrics": [
        "MS Маржа товаров добавленных из поиска (не пустого)"
    ],
    "query": "
        ms нужны заказы
    "},
    {"metrics": [
        "Количество выдач пустого поиска (рекомендации в пустом поиске)"
    ],
    "query": "
        SELECT COUNT(id_search) as id_search
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND search_bar = ''
            AND id_element in ('start', 'restart');
    "},
    {"metrics": [
        "Количество выдач пустого поиска (рекомендации в пустом поиске) с добавлением"
    ],
    "query": "
        SELECT COUNT(id_search) as id_search
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND search_bar = ''
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "Количество товаров, добавленных из пустого поиска"
    ],
    "query": "
        SELECT COUNT(id_tov) as id_tov
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            )
            AND search_bar = '';
    "},
    {"metrics": [
        "Количество разных запросов без добавления (ни одного добавления за период)"
    ],
    "query": "
        WITH ss as (SELECT COUNT(DISTINCT search_bar) as sss
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list})SELECT (select sss from ss) - COUNT(DISTINCT search_bar) as search_bar
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "Количество выдач 'ичего не найдено'",
        "Количество запросов 'Ничего не найдено'"
    ],
    "query": "
        SELECT COUNT(id_search)  as id_search,
            COUNT(search_bar) as search_bar
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in ('restart')
            AND rn_max = 0;
    "},
    {"metrics": [
        "Количество выдач с интересом: любое действие: добавление, просмотр карточки товара, привозите больше, избранное, списки"
    ],
    "query": "
        SELECT source,
            id_element,
            COUNT(id_search) as id_search
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "Средняя позиция добавления",
        "Количество запросов со средней позицией добавления более 6",
        "Количество запросов со средней позицией добавления более 12",
        "Количество запросов со средней позицией добавления более 40"
    ],
    "query": "
        SELECT COUNT(search_bar)                            as search_bar,
            avg(toUInt16OrNull(action))                  as actions,
            sum(if(toUInt16OrNull(action) > 6, 1, 0))    as s_b_6,
            sum(if(toUInt16OrNull(action) > 12, 1, 0))   as s_b_12,
            sum(if(toUInt16OrNull(action) > 40, 1, 0))   as s_b_40
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND toUInt16OrNull(action) > 0
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "Количество уникальных выдач с позицией добавления более 6",
        "Количество уникальных выдач с позицией добавления более 12",
        "Количество уникальных выдач с позицией добавления более 40"
    ],
    "query": "
        SELECT COUNT(DISTINCT id_search)                    as id_search,
            avg(toUInt16OrNull(action))                  as actions,
            sum(if(toUInt16OrNull(action) > 6, 1, 0))    as a_6,
            sum(if(toUInt16OrNull(action) > 12, 1, 0))   as a_12,
            sum(if(toUInt16OrNull(action) > 40, 1, 0))   as a_40
        FROM default.v_tovs_search_
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
        AND toUInt16OrNull(action) > 0
        AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "},
    {"metrics": [
        "Среднее время от запроса до добавления"
    ],
    "query": "
        SELECT min(date_add) as min_d
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND sipHash64(number, id_search) in (SELECT sipHash64(number, id_search)
                                            FROM default.v_tovs_search_new
                                            WHERE date_add > '2022-08-01'
                                                AND id_element in (
                                                                    ''add'',
                                                                    ''plus'',
                                                                    ''minus'',
                                                                    ''edit''
                                                ))
        GROUP BY id_search
        LIMIT 100;

        SELECT min(date_add) as action_d
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND sipHash64(number, id_search) in (SELECT sipHash64(number, id_search)
                                            FROM default.v_tovs_search_new
                                            WHERE date_add between ''{date_1}'' and ''{date_2}''
                                                    AND number in {number_list}
                                                    AND id_element in (
                                                                    ''add'',
                                                                    ''plus'',
                                                                    ''minus'',
                                                                    ''edit''
                                                ))
        GROUP BY number, id_search, id_tov
        ORDER BY id_search, id_tov
        LIMIT 100;
    "},
    {"metrics": [
        "Количество поисковых сессий (Уточнение запроса или исправления ошибки - единая поисковая сессия)"
    ],
    "query": "
        SELECT COUNT(DISTINCT number, id_search) as all_searches
        FROM default.v_tovs_search_new;
    "},
    {"metrics": [
        "Количество сессий без добавления, 30 - 31 используется id_search так как необходимо дологирование очистки поисковой строки."
    ],
    "query": "
        SELECT COUNT(DISTINCT number, id_search) as all_searches
        FROM default.v_tovs_search_new
        WHERE date_add between ''{date_1}'' and ''{date_2}''
            AND number in {number_list}
            AND id_element in (
                            ''add'',
                            ''plus'',
                            ''minus'',
                            ''edit''
            );
    "}
]
