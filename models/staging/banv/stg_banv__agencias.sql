with
    fonte_agencias as (
        select 
            cod_agencia
            , nome
            , endereco
            , cidade
            , uf
            , data_abertura
            , tipo_agencia        
        from {{ source('banv', 'agencias') }}
    )

select *
from fonte_agencias