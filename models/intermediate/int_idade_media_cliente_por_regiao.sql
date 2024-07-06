with
    stg_clientes as (
        select *
        from {{ ref('stg_banv__clientes') }}
    ),

    stg_contas as (
        select *
        from {{ ref('stg_banv__contas') }}
    ),

    stg_agencia as (
        select *
        from {{ ref('stg_banv__agencias') }}
    ),

    joined_tabelas as (
        select
            stg_clientes.nome_cliente,
            stg_clientes.data_nascimento,
            stg_clientes.endereco,
            stg_clientes.email,
            stg_contas.tipo_conta,
            stg_contas.saldo_total,
            stg_contas.saldo_disponivel,
            stg_clientes.data_inclusao,
            stg_agencia.uf,
            extract(year from current_date()) - extract(year from stg_clientes.data_nascimento) - 
            case
                when extract(month from current_date()) < extract(month from stg_clientes.data_nascimento) or 
                     (extract(month from current_date()) = extract(month from stg_clientes.data_nascimento) and extract(day from current_date()) < extract(day from stg_clientes.data_nascimento)) then 1
                else 0
            end as idade
            
        from stg_clientes
        left join stg_contas on 
            stg_contas.cod_cliente = stg_clientes.cod_cliente
        left join stg_agencia on
            stg_agencia.cod_agencia = stg_contas.cod_agencia
    )

select
    uf as regiao,
    avg(idade) as idade_media_clientes
from joined_tabelas
group by uf
