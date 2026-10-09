-- Готівка: fare.tip = null, не 0. Суми це переживають через coalesce, колонка — ні.
select ride_id, payment_method, tip_amount
from {{ ref('rides') }}
where payment_method = 'cash'
  and tip_amount is not null
