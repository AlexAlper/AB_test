SELECT 
    trim(lowerUTF8(search_bar)) as search_bar,
    COUNT(DISTINCT number) as cout_numbers, 
    COUNT(1) as cout_id_search,
    round(avg(toUInt16OrNull(action)),2) as avg_position,
    round(avg(rn_max),2) as avg_rn_max
FROM logs.tovs_search 
WHERE date_add between '{date_1}' AND '{date_2}' AND number in number_list 
GROUP BY search_bar