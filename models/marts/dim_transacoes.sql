with
    stg_transacoes as (
        select *
        from {{ ref('stg_banv__transacoes') }}
    )

    , stg_contas as (
        select* 
        from {{ ref('stg_banv__contas') }}
    )
    
    , stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )

    , joined_tabelas as (
        select
        stg_clientes.nome_cliente
        , stg_clientes.cod_cliente 
        , stg_transacoes.data_transacao 
        , stg_transacoes.data_transacao
        , stg_transacoes.valor_transacao
        , stg_contas.tipo_conta
        , stg_contas.saldo_total
        , stg_contas.saldo_disponivel
        from stg_transacoes
        left join stg_contas on 
        stg_transacoes.num_conta = stg_contas.num_conta
        left join stg_clientes on
        stg_contas.cod_cliente = stg_clientes.cod_cliente
        
    )
    
    , agregar_tabelas as (
        select
            nome_cliente
            , cod_cliente
            , count(*) as numero_transacoes
            , sum(valor_transacao) as total_gasto
            , max(saldo_total) as saldo_total
            , max(saldo_disponivel) as saldo_disponivel
        from joined_tabelas
        group by cod_cliente, nome_cliente 
    )

    
select * 
from agregar_tabelas

    