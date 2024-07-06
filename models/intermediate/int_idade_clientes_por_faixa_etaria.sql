with
    stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    ),

    -- idade dos clientes
    idade_clientes as (
        select
            nome_cliente,
            data_nascimento,
            extract(year from current_date()) - extract(year from data_nascimento) - 
            case
                when extract(month from current_date()) < extract(month from data_nascimento) or 
                     (extract(month from current_date()) = extract(month from data_nascimento) and extract(day from current_date()) < extract(day from data_nascimento)) then 1
                else 0
            end as idade
        from stg_clientes
    ),

    -- Categoriza os clientes em faixas etárias
    clientes_por_faixa_etaria as (
        select
            nome_cliente,
            data_nascimento,
            idade,
            case
                when idade between 18 and 25 then '18-25 anos'
                when idade between 26 and 35 then '26-35 anos'
                when idade between 36 and 45 then '36-45 anos'
                when idade between 46 and 55 then '46-55 anos'
                else 'Mais de 55 anos'
            end as faixa_etaria
        from idade_clientes
    )

select *
from clientes_por_faixa_etaria