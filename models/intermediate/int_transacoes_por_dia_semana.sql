with stg_transacoes as (
    select *
    from {{ ref('stg_banv__transacoes') }}
),

metricas_diarias as (
    select
        extract(dayofweek from cast(data_transacao as timestamp)) as dia_semana,
        count(cod_transacao) as volume_transacoes,
        avg(valor_transacao) as valor_medio_transacoes
    from stg_transacoes
    group by dia_semana
)

select
    case
        when dia_semana = 1 then 'Domingo'
        when dia_semana = 2 then 'Segunda-feira'
        when dia_semana = 3 then 'Terça-feira'
        when dia_semana = 4 then 'Quarta-feira'
        when dia_semana = 5 then 'Quinta-feira'
        when dia_semana = 6 then 'Sexta-feira'
        when dia_semana = 7 then 'Sábado'
    end as dia_da_semana,
    volume_transacoes,
    valor_medio_transacoes
from metricas_diarias
order by dia_semana