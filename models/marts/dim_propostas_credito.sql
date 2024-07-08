with
    stg_propostas_credito as (
        select *
        from {{ ref('stg_banv__propostas_credito') }}
    )

    , stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )
    
    , joined_tabelas as (
        select
            stg.clientes.cod_cliente
            , stg.clientes.nome_cliente
            , stg_proposta_credito.cod_cliente
            , stg_proposta_credito.valor_proposta
            , stg_proposta_credito.valor_financiamento
            , stg_proposta_credito.valor_entrada
            , stg_proposta_credito.quantidade_parcelas
            , stg_proposta_credito.carencia
            , stg_proposta_credito.status_proposta
        from stg_proposta_credito
            left join stg_clientes on stg_proposta_credito.cod_cliente = stg.clientes.cod_cliente
    )
       
select *
from joined_tabelas