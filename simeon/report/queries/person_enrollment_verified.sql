SELECT 
    "{course_id}" as course_id, 
    user_id,
    min(time) as verified_enroll_time,
    IF(max(time) = min(time), NULL, max(time)) as verified_unenroll_time
FROM `{latest_dataset}.enrollment_events`
WHERE ((mode = 'verified' and deactivated) or -- Unenrolled
        (mode='verified' and not activated and mode_changed) -- Enrolled
        )
GROUP BY course_id, user_id
ORDER BY verified_enroll_time asc