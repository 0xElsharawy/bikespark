select
    {{ dbt_utils.generate_surrogate_key([
        'starttime',
        'stoptime',
        'bike_id',
        'start_station_id',
        'end_station_id'
    ]) }} as trip_id,

    tripduration,
    starttime,
    stoptime,

    start_station_id,
    end_station_id,
    bike_id,

    {{ dbt_utils.generate_surrogate_key([
        'usertype',
        'birth_year',
        'gender'
    ]) }} as user_id

from {{ ref('stg_trips') }}
