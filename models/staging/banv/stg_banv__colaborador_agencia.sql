with 
    fonte_colaborador_agencia as (

        select
            cod_colaborador
            , cod_agencia
        from {{ source('banv', 'colaborador_agencia') }}

    )
     
select *
from fonte_colaborador_agencia
