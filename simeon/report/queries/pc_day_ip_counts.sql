SELECT 
    username, 
    ip, 
    date(time) as date, 
    count(*) as ipcount,
    '{course_id}' as course_id,
FROM `{log_dataset}.tracklog_*`
where username != ""
group by username, ip, date
order by date 