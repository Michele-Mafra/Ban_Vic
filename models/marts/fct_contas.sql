with stg_contas as (
    select *
    from {{ ref('stg_banv__contas') }}
),

stg_agencia as (
    select *
    from {{ ref('stg_banv__agencias') }}
),

tipo_agencia as (
    select
        stg_agencia.cod_agencia,
        stg_agencia.nome as nome_agencia,
        stg_agencia.tipo_agencia,
        stg_contas.tipo_conta,
        count(*) as quantidade_contas,
        case
            when stg_agencia.tipo_agencia = 'Digital' then 'Digital'
            when stg_agencia.tipo_agencia = 'Fisica' then 'Fisica'
            else 'Outro'
         end as tipo_agencia_categoria
    from stg_contas
    left join stg_agencia on stg_agencia.cod_agencia = stg_contas.cod_agencia
    where stg_agencia.tipo_agencia in ('Digital', 'Fisica') -- Filtra por tipo de agência digital e física
    group by stg_agencia.cod_agencia, stg_agencia.nome, stg_agencia.tipo_agencia, stg_contas.tipo_conta
)

select * from tipo_agencia