with source as (
    select * from {{ source('external_raw', 'raw_orders') }}
),

renamed_and_cast as (
    select
        -- Identificadores (Llaves naturales)
        cast(order_id as varchar) as order_id,
        cast(user_id as integer) as user_id,
        
        -- Dimensiones y atributos categóricos
        cast(country_code as varchar(2)) as country_code,
        cast(order_status as varchar(30)) as order_status,
        
        -- Métricas aditivas
        cast(total_amount as decimal(10, 2)) as total_amount_usd,
        
        -- Timestamps con zona horaria normalizada
        cast(created_at as timestamp) as created_at_utc,
        
        -- Metadatos de auditoría y linaje (Critical Data Engineering metadata)
        current_timestamp as ingested_at_utc

    from source
)

select * from renamed_and_cast