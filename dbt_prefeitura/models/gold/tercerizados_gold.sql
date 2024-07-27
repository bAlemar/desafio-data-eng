with base_data as (
    select
        *
    from {{ ref('tercerizados_silver') }}
)

-- Seleção final dos dados
select
 *
from base_data
