SELECT 
    time, 
    username,
    '{course_id}' as course_id,
    module_id,
FROM `{{ log_dataset }}.tracklog_*`
WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
(event_type = "show_answer" or event_type = "showanswer")
ORDER BY time