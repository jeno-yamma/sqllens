select *
from ds_prod.staging._stg_page_screen_views_sessionized
where 
    event_date = '2022-01-01'
    and impression_type = 0
;
