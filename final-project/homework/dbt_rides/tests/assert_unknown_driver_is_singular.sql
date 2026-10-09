-- Член 'unknown' має лишатися рівно одним після будь-якої кількості інкрементів.
select count(*) as unknown_rows
from {{ ref('dim_driver') }}
where driver_key = 'unknown'
having count(*) <> 1
