with
    clientes as (
        select*
        from {{ ref('dim_clientes') }}
    ) 

   , agencia as (
        select *
        from {{ ref('dim_agencia') }}
    )

    , contas as (
        select*
        from {{ ref('dim_contas') }}
    )

    , int_clientes_contas as (
        select *
        from {{ ref('int_marketing__clientes_contas') }}

    )

    , int_moviemtacao_transacoes_contas as (
        select *
        from {{ ref('int_moviemtacao_transacoes_contas') }}
    )
    
    , joined_tabelas as (
        select
        int_clientes_contas.cod_cliente
        , int_clientes_contas.cod_agencia
        , int_clientes_contas.cod_colaborador
        , int_clientes_contas.tipo_conta
        , int_clientes_contas.data_abertura
        , int_clientes_contas.saldo_total
        , int_clientes_contas.saldo_disponivel
        , int_clientes_contas.data_ultimo_lancamento
        , int_clientes_contas.nome_cliente
        , int_clientes_contas.tipo_cliente
        , int_clientes_contas.data_inclusao
        , int_clientes_contas.cpfcnpj
        , int_clientes_contas.data_nascimento
        , int_clientes_contas.endereco
        , int_clientes_contas.cep
        , int_moviemtacao_transacoes_contas.num_conta   
        , int_moviemtacao_transacoes_contas.nome_transacao
        , int_moviemtacao_transacoes_contas.valor_transacao
        


        from int_clientes_contas
        left join int_moviemtacao_transacoes_contas on
            int_clientes_contas.cod_cliente = int_moviemtacao_transacoes_contas.cod_cliente
             

    )

    
    
            
            


select *
from joined_tabelas

