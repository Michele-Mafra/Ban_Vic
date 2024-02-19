with
    stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    )

    , stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )

    , joined_tabelas as (
        select
            stg_contas.cod_cliente
            , stg_contas.cod_agencia
            , stg_contas.cod_colaborador
            , stg_contas.tipo_conta
            , stg_contas.data_abertura
            , stg_contas.saldo_total
            , stg_contas.saldo_disponivel
            , stg_contas.data_ultimo_lancamento            
            , stg_clientes.nome_cliente           
            , stg_clientes.tipo_cliente
            , stg_clientes.data_inclusao
            , stg_clientes.cpfcnpj
            , stg_clientes.data_nascimento
            , stg_clientes.endereco
            , stg_clientes.cep
        from stg_contas
        left join stg_clientes on
            stg_clientes.cod_cliente = stg_contas.cod_cliente
    )

    
    

select *
from joined_tabelas
    
    

    