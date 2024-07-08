WITH 
    stg_transacoes as (
        select *
        from {{ ref('stg_banv__transacoes') }}
    )

    , stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    )

         
    , fct_gastos_clientes AS (
        SELECT
            num_conta,
            SUM(valor_transacao) AS total_gasto,
            COUNT(*) AS numero_transacoes
        FROM stg_transacoes
        GROUP BY num_conta, nome_transacao
    )

select * 
from fct_gastos_clientes

