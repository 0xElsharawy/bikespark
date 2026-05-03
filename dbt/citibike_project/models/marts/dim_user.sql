select
    {{ dbt_utils.generate_surrogate_key([
        'usertype',
        'birth_year',
        'gender'
    ]) }} as user_id,

    usertype,
    birth_year,
    gender

from {{ ref('stg_trips') }}

group by
    usertype,
    birth_year,
    gender
