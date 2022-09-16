SELECT 
    search_bar as search_bar,
    COUNT(DISTINCT number) as cout_numbers, 
    COUNT(1) as cout_id_search,
    avg(position) as avg_position,
    avg(rn_max) as avg_rn_max
FROM logs.tovs_search 
WHERE date_add between '{date_1}' AND '{date_2}' AND number in number_list 
GROUP BY search_bar