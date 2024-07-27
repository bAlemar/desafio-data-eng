-- cria cte para transformar

-- models/silver/tercerizados_silver.sql

with transformed_data as (
    select
        *,
        -- Converte 'Ano_Carga' de inteiro para uma data representando o primeiro dia do ano
        date_trunc('year', to_date(cast("Ano_Carga" as text), 'YYYY')) as ano_date
    from {{ ref('tercerizados_bronze') }}
)

select
    *
from transformed_data

