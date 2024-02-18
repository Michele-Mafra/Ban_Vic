with
    stg_propostas_credito as (
        select *
        from {{ ref('stg_banv__propostas_credito') }}
    )

select * 
from stg_propostas_credito