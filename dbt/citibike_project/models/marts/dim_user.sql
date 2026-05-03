select
    {{ dbt_utils.generate_surrogate_key([
        'usertype',
        'coalesce(birth_year, 0)',
        'gender'
    ]) }} as user_id,

    usertype,
    coalesce(birth_year, 0) as birth_year,
    gender

from {{ ref('stg_trips') }}

group by
    usertype,
    birth_year,
    gender
