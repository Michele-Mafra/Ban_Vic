with
    stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    )

select * 
from stg_contas