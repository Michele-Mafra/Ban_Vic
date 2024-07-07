with stg_transacoes as (
    select *
    from {{ ref('stg_banv__transacoes') }}
),

metricas_diarias as (
    select
        data_transacao,
        valor_transacao,
        extract(day from cast(data_transacao as timestamp)) as dia
    from stg_transacoes
)

select
    case
        when dia <= 15 then 'Início de mês'
        else 'Final de mês'
    end as periodo,
    avg(valor_transacao) as media_valor_movimentado
from metricas_diarias
group by periodo
order by media_valor_movimentado desc

