with stg_transacoes as (
    select *
    from {{ ref('stg_banv__transacoes') }}
)

select
    case
        when extract(day from timestamp(data_transacao)) <= 15 then 'Inicio do Mes'
        else 'Final do Mes'
    end as periodo_mes,
    avg(valor_transacao) as valor_medio_transacoes
from stg_transacoes
group by periodo_mes
order by periodo_mes


