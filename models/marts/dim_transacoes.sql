with
    stg_transacoes as (
        select *
        from {{ ref('stg_banv__transacoes') }}
    )

select * 
from stg_transacoes