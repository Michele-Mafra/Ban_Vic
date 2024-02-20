with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )


    --Quantidade de clientes
    , qtd_clientes as (
        select
            count(*) as qtd_clientes
        from {{ source('banv', 'clientes') }} 
        
    
    )
--ver idade dos clientes   
    , idade_cliente as (
        SELECT 
            sum(DATE_DIFF('2024-02-19',data_nascimento, YEAR)) AS idade_media_cliente
        from {{ source('banv', 'clientes') }}
    )
--ver media de idade dos clientes   
    --, media_idade_cliente as (
        --SELECT
            --AVG(idade_cliente) AS media_idade
        --from {{ source('banv', 'clientes') }}
       -- WHERE age IN (FLOOR(count / 2), CEIL(count / 2))
    --)
     



    


select*
from 
    qtd_clientes
    , idade_cliente


 