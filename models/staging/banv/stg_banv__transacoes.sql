with 
    fonte_transacoes as (

        select 
            cod_transacao
            , num_conta
            , data_transacao
            , nome_transacao
            , valor_transacao

        from {{ source('banv', 'transacoes') }}

    )



select * 
from fonte_transacoes
