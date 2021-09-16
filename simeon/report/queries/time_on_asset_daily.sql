SELECT
    "{{ course_id }}" as course_id,
    date(time) as date,
    username,
    module_id,
    -- time_umid5 = total time on module (by module_id) in seconds
    -- time_mid5 has 5 minute timeout, time_mid30 has 30 min timeout
    CAST(SUM(case when dt_umid < 5*60 then dt_umid end) AS FLOAT64) as time_umid5,
    CAST(SUM( case when dt_umid < 30*60 then dt_umid end) AS FLOAT64) as time_umid30,
FROM (
    SELECT time,
        username,
        module_id,
        TIMESTAMP_DIFF(time, last_time, SECOND) as dt,
        TIMESTAMP_DIFF(time, last_time_umid, SECOND) as dt_umid,
        last_time_umid,
    FROM (
        SELECT time,
            username,
            last_username,
            module_id,
            last_time,
            last_time_umid,                    
        FROM (
            SELECT time,
                username,
                module_id,
                lag(time, 1) over (partition by username order by time) last_time,
                lag(username, 1) over (partition by username order by time) last_username,
                lag(time, 1) over (partition by username, module_id order by time) last_time_umid,
            FROM (
                SELECT time,
                    username,
                    (case when REGEXP_CONTAINS(module_id, r'.*\"\}}$') then REGEXP_EXTRACT(module_id, r'(.*)\"\}}$')
                    when REGEXP_CONTAINS(module_id, r'.*\"\]\}}\}}$') then REGEXP_EXTRACT(module_id, r'(.*)\"\]\}}\}}$')
                    else module_id end) as module_id
                FROM `{{log_dataset}}.tracklog_*`
{% if suffix_start is defined and suffix_end is defined %}
                WHERE _TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}"
{% endif %}
            )
        WHERE
        module_id is not null
        AND username is not null
        AND username != ''
        )
    )
)
WHERE module_id is not null
AND module_id NOT LIKE '%"%'
GROUP BY date, module_id, username
ORDER BY date, module_id, username