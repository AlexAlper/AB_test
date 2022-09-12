WITH search_add_all as (
SELECT COUNT(id_search) as add_all FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' AND id_element in ( 'add', 'plus', 'minus', 'edit')
), number_all as (
SELECT COUNT(number) as add_all FROM logs.tovs_search
), search_all as (
SELECT COUNT(id_search) as add_all FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}'
), search_bar_add_all as (
SELECT search_bar, COUNT(id_search) as add_all FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' AND id_element in ( 'add', 'plus', 'minus', 'edit') GROUP BY search_bar
)
SELECT search_bar as search_bar, COUNT(DISTINCT number) as cout_numbers, COUNT(id_search) as cout_id_search, search_add_all.add_all as search_add_all_, number_all.add_all as number_all_,
search_all.add_all as search_all_, search_bar_add_all.add_all as search_bar_add_all_, cout_id_search / search_bar_add_all_ as konv_, avg(position) as avg_position,
avg(rn_max) as avg_rn_max
FROM logs.tovs_search 
INNER JOIN search_add_all ON 1=1
INNER JOIN number_all ON 1=1
INNER JOIN search_all ON 1=1
LEFT JOIN search_bar_add_all on tovs_search.search_bar = search_bar_add_all.search_bar
WHERE date_add between '{date_1}' and '{date_2}' AND number in number_list 
GROUP BY search_bar, search_add_all_, number_all_, search_all_, search_bar_add_all_