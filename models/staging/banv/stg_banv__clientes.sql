with
    fonte_clientes as (
        select 
            cod_cliente
            , primeiro_nome || ' ' || ultimo_nome as nome
            --, email
            , tipo_cliente
            --, data_inclusao
            , cpfcnpj
            , data_nascimento
            , endereco
            , cep
        from {{ source('banv', 'clientes') }}
    )
select *
from fonte_clientes