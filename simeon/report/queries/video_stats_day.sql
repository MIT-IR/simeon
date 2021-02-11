SELECT 
    date(time)as date, 
    username,
    case 
        when REGEXP_CONTAINS( JSON_EXTRACT(event, '$.id') , r'([-])' ) then REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', ''), r'(?:.*\/)(.*)') 
        else REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT(event, '$.id'), '-', '/'), '"', ''), 'i4x/', '') 
    end as video_id, 
    max(
        case 
            when JSON_EXTRACT_SCALAR(event, '$.speed') is not null 
                then CAST(JSON_EXTRACT_SCALAR(event,'$.speed') as FLOAT64) * CAST(JSON_EXTRACT_SCALAR(event, '$.currentTime') as FLOAT64) 
            else  CAST(JSON_EXTRACT_SCALAR(event, '$.currentTime') as FLOAT64) 
        end) as position,
FROM `{log_dataset}.tracklog_*`
WHERE 
    (event_type = "play_video" or event_type = "pause_video" or event_type = "stop_video") 
    and event is not null
GROUP BY username, video_id, date
ORDER BY date