with   
    dados_clientes as(
    select
        timestamp(data_inclusao) AS data_inclusao,
        DATE_TRUNC(timestamp(data_inclusao), year) AS ano,
        nome_cliente,
        cod_cliente,
    from {{ ref('stg_banv__clientes') }}
)

select
    ano,
    count(*) as quantidade_clientes
from dados_clientes
group by ano
order by ano