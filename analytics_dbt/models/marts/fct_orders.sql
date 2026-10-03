{{
    config(
        materialized='incremental',
        unique_key='order_key',
        incremental_strategy='delete+insert'
    )
}}

with staged_orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        -- Surrogate Key determinista
        md5(order_id) as order_key,

        -- Natural Keys
        order_id,
        user_id,

        -- Dimensiones y atributos
        country_code,
        order_status,

        -- Métricas
        total_amount_usd,

        -- Timestamps de evento y de proceso
        created_at_utc,
        ingested_at_utc,
        current_timestamp as mart_updated_at_utc

    from staged_orders

    {% if is_incremental() %}
        -- En ejecuciones subsecuentes, solo procesamos lo que sea más reciente
        -- que el último timestamp registrado en esta tabla
        where created_at_utc > (select max(created_at_utc) from {{ this }})
    {% endif %}
)

select * from final