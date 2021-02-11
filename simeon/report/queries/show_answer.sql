SELECT 
    time, 
    username,
    '{course_id}' as course_id,
    module_id,
    from `{dataset}.tracklog_*`
where (event_type = "show_answer" or event_type = "showanswer")
order by time