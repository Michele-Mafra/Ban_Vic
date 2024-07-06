with
    stg_transacoes as (
        select *
        from {{ ref('stg_banv__transacoes') }}
    )

    , stg_contas as (
        select* 
        from {{ ref('stg_banv__contas') }}
    )
    
    , joined_tabelas as (
        select
        stg_transacoes.nome_transacao
        , stg_transacoes.data_transacao 
        , stg_transacoes.data_transacao
        , stg_transacoes.valor_transacao
        , stg_contas.tipo_conta
        , stg_contas.saldo_total
        , stg_contas.saldo_disponivel
    from stg_transacoes
        left join stg_contas on 
        stg_transacoes.num_conta = stg_contas.num_conta
    )


select * 
from stg_transacoes

    