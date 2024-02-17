with 
    fonte_contas as (

        select 
            num_conta
            , cod_cliente
            , cod_agencia
            , cod_colaborador
            , tipo_conta
            , data_abertura
            , saldo_total
            , saldo_disponivel
            , data_ultimo_lancamento

        
        from {{ source('banv', 'contas') }}

    )


select * 
from fonte_contas
