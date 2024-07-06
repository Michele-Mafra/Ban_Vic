with stg_transacoes as (
    select *
    from {{ ref('stg_banv__transacoes') }}
)

select
    extract(dayofweek from cast(data_transacao as timestamp)) as dia_semana
    , count(cod_transacao) as volume_transacoes
    , avg(valor_transacao) as valor_medio_transacoes
from stg_transacoes
group by dia_semana
order by dia_semana

