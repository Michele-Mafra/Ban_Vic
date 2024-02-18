with
    stg_colaborador_agencia as (
        select
            cod_colaborador
            , cod_agencia 
        from {{ ref('stg_banv__colaborador_agencia') }}
    )

select *
from stg_colaborador_agencia