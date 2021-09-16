SELECT 
    username, 
    ip, 
    date(time) as date, 
    count(*) as ipcount,
    '{course_id}' as course_id,
FROM `{{ log_dataset }}.tracklog_*`
WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
username != ""
GROUP BY username, ip, date
ORDER BY date 