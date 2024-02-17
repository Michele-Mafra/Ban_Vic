with
    stg_clientes as (
        select 
            cod_cliente
            , primeiro_nome || ' ' || ultimo_nome as nome
            --, ultimo_nome
            --, email
            , tipo_cliente
            , data_inclusao
            , cpfcnpj
            , data_nascimento
            , endereco
            , cep
        from {{ ref('stg_banv__clientes') }}
    )

select * 
from stg_clientes