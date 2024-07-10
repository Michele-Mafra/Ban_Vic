with
    stg_agencia as (
        select *
        from {{ ref('stg_banv__agencias') }}
    )
    , stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )

    , stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    )


    
    , joined_tabelas as (
        select
            stg_clientes.nome_cliente
            , stg_clientes.tipo_cliente
            , stg_clientes.endereco
            , stg_clientes.email
            , stg_clientes.data_inclusao
            , stg_contas.tipo_conta
            , stg_contas.saldo_total
            , stg_contas.saldo_disponivel
            , stg_agencia.cod_agencia
            , stg_agencia.nome
            , stg_agencia.cidade
            , stg_agencia.uf
            , stg_agencia.data_abertura
            , stg_agencia.tipo_agencia
          
            
        from stg_clientes
        left join stg_contas on 
            stg_contas.cod_cliente = stg_clientes.cod_cliente
        left join stg_agencia on
            stg_agencia.cod_agencia = stg_contas.cod_agencia
        
    )
    
select *
from joined_tabelas
