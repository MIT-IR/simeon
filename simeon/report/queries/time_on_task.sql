{% set timeout_short = int(timeout_short) if timeout_short is defined and timeout_short else 5 %}
{% set timeout_long = int(timeout_long) if timeout_long is defined and timeout_long else 30 %}
SELECT 
    "{{ course_id }}" as course_id,
    date(time) as date,
    username, 
    -- total time spent on system
    SUM(case when dt < {{ timeout_short * 60.0 }} then dt end) as total_time_{{ timeout_short }},
    SUM(case when dt < {{ timeout_long * 60.0 }} then dt end) as total_time_{{ timeout_long }},
    -- total time spent watching videos
    SUM(case when (dt_video is not null) and (dt_video < {{ timeout_short * 60.0 }}) then dt_video end) as total_video_time_{{ timeout_short }},
    SUM(case when (dt_video is not null) and (dt_video < {{ timeout_long * 60.0 }}) then dt_video end) as total_video_time_{{ timeout_long }},
    SUM(case when (serial_dt_video is not null) and (serial_dt_video < {{ timeout_long * 60.0 }}) then serial_dt_video end) as serial_video_time_{{ timeout_long }},
    -- total time spent doing problems
    SUM(case when (dt_problem is not null) and (dt_problem < {{ timeout_short * 60.0 }}) then dt_problem end) as total_problem_time_{{ timeout_short }},
    SUM(case when (dt_problem is not null) and (dt_problem < {{ timeout_long * 60.0 }}) then dt_problem end) as total_problem_time_{{ timeout_long }},
    SUM(case when (serial_dt_problem is not null) and (serial_dt_problem < {{ timeout_long * 60.0 }}) then serial_dt_problem end) as serial_problem_time_{{ timeout_long }},
    -- total time spent on forum
    SUM(case when (dt_forum is not null) and (dt_forum < {{ timeout_short * 60.0 }}) then dt_forum end) as total_forum_time_{{ timeout_short }},
    SUM(case when (dt_forum is not null) and (dt_forum < {{ timeout_long * 60.0 }}) then dt_forum end) as total_forum_time_{{ timeout_long }},
    SUM(case when (serial_dt_forum is not null) and (serial_dt_forum < {{ timeout_long * 60.0 }}) then serial_dt_forum end) as serial_forum_time_{{ timeout_long }},
    -- total time spent with textbook or wiki
    SUM(case when (dt_text is not null) and (dt_text < {{ timeout_short * 60.0 }}) then dt_text end) as total_text_time_{{ timeout_short }},
    SUM(case when (dt_text is not null) and (dt_text < {{ timeout_long * 60.0 }}) then dt_text end) as total_text_time_{{ timeout_long }},
    SUM(case when (serial_dt_text is not null) and (serial_dt_text < {{ timeout_long * 60.0 }}) then serial_dt_text end) as serial_text_time_{{ timeout_long }},
    FROM (
        SELECT time,
            username,
            CAST(TIMESTAMP_DIFF(time, last_time, SECOND) AS FLOAT64) as dt,         -- dt is in seconds
            case when is_video then CAST(TIMESTAMP_DIFF(time, last_time_video, SECOND) AS FLOAT64) end as dt_video,
            case when is_problem then CAST(TIMESTAMP_DIFF(time, last_time_problem, SECOND) AS FLOAT64) end as dt_problem,
            case when is_forum then CAST(TIMESTAMP_DIFF(time, last_time_forum, SECOND) AS FLOAT64) end as dt_forum,
            case when is_text then CAST(TIMESTAMP_DIFF(time, last_time_text, SECOND) AS FLOAT64) end as dt_text,
            case when is_video then CAST(TIMESTAMP_DIFF(time, last_time_xevent, SECOND) AS FLOAT64) end as serial_dt_video,
            case when is_problem then CAST(TIMESTAMP_DIFF(time, last_time_xevent, SECOND) AS FLOAT64) end as serial_dt_problem,
            case when is_forum then CAST(TIMESTAMP_DIFF(time, last_time_xevent, SECOND) AS FLOAT64) end as serial_dt_forum,
            case when is_text then CAST(TIMESTAMP_DIFF(time, last_time_xevent, SECOND) AS FLOAT64) end as serial_dt_text,
        FROM (
            SELECT time, 
                username,
                last_username,
                last_time,
                (case when is_video then last_time_video end) as last_time_video,
                -- last_username_video,
                -- last_event_video,
                is_problem,
                is_video,
                (case when is_problem then last_time_problem end) as last_time_problem,
                -- last_username_problem,
                -- last_event_problem,
                is_forum,
                is_text,
                (case when is_forum then last_time_forum end) as last_time_forum,
                (case when is_text then last_time_text end) as last_time_text,
                is_xevent,
                (case when is_xevent then last_time_xevent end) as last_time_xevent,
            FROM (
                SELECT time,
                    username,
                    lag(time, 1) over (partition by username order by time) last_time,
                    lag(username, 1) over (partition by username order by time) last_username,
                    is_video,
                    is_problem,
                    is_forum,
                    is_text,
                    (is_video or is_problem or is_forum or is_text) as is_xevent,   -- x = video, problem, forum, or text: any event
                    case when is_problem then username else '' end as uname_problem,
                    case when is_video then username else '' end as uname_video,
                    case when is_forum then username else '' end as uname_forum,
                    case when is_text then username else '' end as uname_text,
                    case when (is_video or is_problem or is_forum or is_text) then username else '' end as uname_xevent,
                    lag(time, 1) over (partition by case when is_video then username else '' end order by time) last_time_video,
                    -- lag(event_type, 1) over (partition by uname_video order by time) last_event_video,
                    -- lag(uname_video, 1) over (partition by uname_video order by time) last_username_video,
                    lag(time, 1) over (partition by case when is_problem then username else '' end order by time) last_time_problem,
                    -- lag(event_type, 1) over (partition by uname_problem order by time) last_event_problem,
                    -- lag(uname_problem, 1) over (partition by uname_problem order by time) last_username_problem,
                    lag(time, 1) over (partition by case when is_forum then username else '' end order by time) last_time_forum,
                    lag(time, 1) over (partition by case when is_text then username else '' end order by time) last_time_text,
                    lag(time, 1) over (partition by case when (is_video or is_problem or is_forum or is_text) then username else '' end order by time) last_time_xevent,
                FROM (
                    SELECT time,
                        username,
                        event_type,
                        REGEXP_CONTAINS(event_type, r'\w+_video|\w+_transcript') as is_video,
                        REGEXP_CONTAINS(event_type, r'problem_\w+') as is_problem,
                        REGEXP_CONTAINS(
                            event_type,
                            r'^edx\.forum\..*|/discussion/forum|/discussion/threads|/discussion/comments'
                        ) as is_forum,
                        REGEXP_CONTAINS(event_type, r'^textbook\..*|/wiki/') as is_text
                    FROM `{{log_dataset}}.tracklog_*`
                    WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
                    NOT REGEXP_CONTAINS(event_type, "/xblock/") AND username is not null AND username != ''
                )
            )
            WHERE last_time is not null
            ORDER BY username, time
        )
    )
group by course_id, username, date
order by date, username