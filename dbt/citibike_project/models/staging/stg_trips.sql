select
    tripduration,
    starttime,
    stoptime,
    start_station_id,
    start_station_name,
    start_station_latitude as start_latitude,
    start_station_longitude as start_longitude,
    end_station_id,
    end_station_name,
    end_station_latitude as end_latitude,
    end_station_longitude as end_longitude,
    bikeid as bike_id,
    usertype,
    coalesce(birth_year, 0) as birth_year,
    gender
from {{ source('citibike', 'raw_trips') }}
