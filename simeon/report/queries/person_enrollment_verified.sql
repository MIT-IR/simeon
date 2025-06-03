SELECT 
    course_id,
    user_id,
    min(verified_enroll_time) as verified_enroll_time,
    max(verified_unenroll_time) as verified_unenroll_time
    FROM
        (
            SELECT 
                "{course_id}" as course_id, 
                user_id, 
                time as verified_enroll_time,
                null as verified_unenroll_time
            FROM `{latest_dataset}.enrollment_events`
            WHERE 
                mode = "verified" and mode_changed
            UNION ALL
            SELECT 
                "{course_id}" as course_id, 
                user_id, 
                null as verified_enroll_time,
                time as verified_unenroll_time
            FROM `{latest_dataset}.enrollment_events`
            WHERE 
                mode = "verified" and deactivated
        )
GROUP BY course_id, user_id
