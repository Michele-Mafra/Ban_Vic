with
    stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )

    , stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    )

    , joined_tabelas as (
        select
            stg_clientes.primeiro_nome
            , stg_clientes.tipo_cliente
            , stg_clientes.endereco
            , stg_clientes.email
            , stg_contas.tipo_conta
            , stg_contas.saldo_total
            , stg_contas.saldo_disponivel
        from stg_clientes
        left join stg_contas on 
            stg_contas.cod_cliente = stg_clientes.cod_cliente
    )
    
select *
from joined_tabelas
