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
    FROM `{log_dataset}.tracklog_*`
    where (event_type = "edx.course.enrollment.activated") or
            (event_type = "edx.course.enrollment.deactivated") or
            (event_type = "edx.course.enrollment.mode_changed")
    order by time;