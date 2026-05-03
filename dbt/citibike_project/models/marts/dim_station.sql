with stations as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        start_latitude as latitude,
        start_longitude as longitude
    from {{ ref('stg_trips') }}

    union all

    select
        end_station_id,
        end_station_name,
        end_latitude,
        end_longitude
    from {{ ref('stg_trips') }}

),

deduped as (

    select distinct
        station_id,
        station_name,
        latitude,
        longitude
    from stations
    where station_id is not null

)

select * from deduped
