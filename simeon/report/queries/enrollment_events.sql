 SELECT 
    "{course_id}" as course_id,
    time,
    IFNULL(event_struct.user_id, context.user_id) as user_id,
    IFNULL(event_struct.mode, JSON_EXTRACT_SCALAR(event, "$.mode")) as mode,
    IF(event_type = "edx.course.enrollment.activated", True, False) as activated,
    IF(event_type = "edx.course.enrollment.deactivated", True, False) as deactivated,
    IF(event_type = "edx.course.enrollment.mode_changed", True, False) as mode_changed,
    IF(event_type = "edx.course.enrollment.upgrade.succeeded", True, False) as upgraded,
    event_type
FROM `{{ log_dataset }}.tracklog_*`
WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
event_type like "%edx.course.enrollment%"
order by time