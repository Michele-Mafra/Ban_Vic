with
    stg_transacoes as (
        select *
        from {{ ref('stg_banv__transacoes') }}
    )

    , stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    )

    , joined_tabelas as (
        select
            stg_transacoes.num_conta
            , stg_transacoes.cod_transacao
            , stg_transacoes.nome_transacao
            , stg_transacoes.valor_transacao
            , stg_contas.cod_cliente
            , stg_contas.cod_agencia
            , stg_contas.cod_colaborador
            , stg_contas.tipo_conta
            , stg_contas.data_abertura
            , stg_contas.saldo_total
            , stg_contas.saldo_disponivel
            , stg_contas.data_ultimo_lancamento
        
        from stg_transacoes
        left join stg_contas on
            stg_transacoes.num_conta = stg_contas.num_conta
    )

    
    

select *
from joined_tabelas
