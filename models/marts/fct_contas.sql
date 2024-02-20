with
    contas as (
        select *
        from {{ ref('dim_contas') }}
    )
    -- total de contas abertas em 2010
    , total_contas_2010 as (
        select count(*) as total_contas_2010
        from {{ ref('dim_contas') }}
        where data_abertura between '2010-01-01' and '2010-12-31'
    )

       -- total de contas abertas em 2011
    , total_contas_2011 as (
        select count(*) as total_contas_2011
        from {{ ref('dim_contas') }}
        where data_abertura between '2011-01-01' and '2011-12-31'
    )
    -- total de contas abertas em 2012
    , total_contas_2012 as (
        select count(*) as total_contas_2012
        from {{ ref('dim_contas') }}
        where data_abertura between '2012-01-01' and '2012-12-31'
    )
    -- total de contas abertas em 2013
    , total_contas_2013 as (
        select count(*) as total_contas_2013
        from {{ ref('dim_contas') }}
        where data_abertura between '2013-01-01' and '2013-12-31'
    )
    -- total de contas abertas em 2014  
    , total_contas_2014 as (
        select count(*) as total_contas_2014
        from {{ ref('dim_contas') }}
        where data_abertura between '2014-01-01' and '2014-12-31'
    )
    -- total de contas abertas em 2015  
    , total_contas_2015 as (
        select count(*) as total_contas_2015
        from {{ ref('dim_contas') }}
        where data_abertura between '2015-01-01' and '2015-12-31'
    )
    -- total de contas abertas em 2016  
    , total_contas_2016 as (
        select count(*) as total_contas_2016
        from {{ ref('dim_contas') }}
        where data_abertura between '2016-01-01' and '2016-12-31'
    )
    -- total de contas abertas em 2017  
    , total_contas_2017 as (
        select count(*) as total_contas_2017
        from {{ source('banv', 'contas') }}
        where data_abertura between '2017-01-01' and '2017-12-31'
    )
    -- total de contas abertas em 2018    
    , total_contas_2018 as (
        select count(*) as total_contas_2018
        from {{ ref('dim_contas') }}
        where data_abertura between '2018-01-01' and '2018-12-31'
    )
    -- total de contas abertas em 2019    
    , total_contas_2019 as (
        select count(*) as total_contas_2019
        from {{ ref('dim_contas') }}
        where data_abertura between '2019-01-01' and '2019-12-31'
    )
    -- total de contas abertas em 2020    
    , total_contas_2020 as (
        select count(*) as total_contas_2020
        from {{ ref('dim_contas') }}
        where data_abertura between '2020-01-01' and '2020-12-31'
    )
    -- total de contas abertas em 2021
    , total_contas_2021 as (
        select count(*) as total_contas_2021
        from {{ ref('dim_contas') }}
        where data_abertura between '2021-01-01' and '2021-12-31'
    )
    -- total de contas abertas em 2022
    , total_contas_2022 as (
        select count(*) as total_contas_2022
        from {{ ref('dim_contas') }}
        where data_abertura between '2022-01-01' and '2022-12-31'
    )


    
    

select *
from 
    total_contas_2010
    , total_contas_2011
    , total_contas_2012
    , total_contas_2013
    , total_contas_2014
    , total_contas_2015
    , total_contas_2016
    , total_contas_2017
    , total_contas_2018
    , total_contas_2019
    , total_contas_2020
    , total_contas_2021
    , total_contas_2022
    

