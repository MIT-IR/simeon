SELECT 
    "{course_id}" as course_id,
    time, 
    event_struct.user_id as user_id, 
    case when (event_type = "edx.course.enrollment.activated" 
                and event_struct.mode = "honor")
            then 1 
            when (event_type = "edx.course.enrollment.deactivated" 
                and event_struct.mode = "honor")
            then -1 
            else 0 end as diff_enrollment_honor,
    case when (event_type = "edx.course.enrollment.activated" 
                and event_struct.mode = "verified")
            then 1 
            when (event_type = "edx.course.enrollment.deactivated" 
                and event_struct.mode = "verified")
            then -1 
            when (event_type = "edx.course.enrollment.mode_changed" 
                and event_struct.mode = "verified")
            then 1 
            when (event_type = "edx.course.enrollment.mode_changed" 
                and event_struct.mode = "honor")
            then -1 
            else 0 end as diff_enrollment_verified,
    case when (event_type = "edx.course.enrollment.activated" 
                and event_struct.mode = "audit")
            then 1 
            when (event_type = "edx.course.enrollment.deactivated" 
                and event_struct.mode = "audit")
            then -1 
            else 0 end as diff_enrollment_audit,
    FROM `{{ log_dataset }}.tracklog_*`
    WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
    (event_type = "edx.course.enrollment.activated") OR (event_type = "edx.course.enrollment.deactivated") OR (event_type = "edx.course.enrollment.mode_changed")
    order by time;