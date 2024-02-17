with 
    fonte_colaboradores as (

        select 
            cod_colaborador
            , primeiro_nome || ' ' || ultimo_nome as nome
            --, ultimo_nome
            --, email
            , cpf
            , data_nascimento
            , endereco
            , cep

        from {{ source('banv', 'colaboradores') }}

    )


select * 
from fonte_colaboradores
