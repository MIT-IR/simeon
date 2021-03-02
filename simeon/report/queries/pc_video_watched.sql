SELECT 
    user_id, 
    "{course_id}" as course_id,
    count(*) n_unique_videos_watched,
    safe_divide(count(*), n_total_videos) as fract_total_videos_watched,
    certified, verified
FROM
    (
        SELECT 
            UIC.user_id as user_id, 
            UV.username as username,
            video_id, 
            n_views,
            NV.n_total_videos as n_total_videos,
            case when uic.certificate_status = "downloadable" then True else False end as certified,
            (UIC.enrollment_mode="verified") as verified,
        FROM
            (
                SELECT 
                    username, 
                    video_id, 
                    count(*) as n_views
                FROM `{latest_dataset}.video_stats_day`
                GROUP BY username, video_id
            ) UV
        JOIN `{latest_dataset}.user_info_combo` UIC
        on UV.username = UIC.username
        JOIN `{latest_dataset}.roles` ROL
        on ROL.user_id = UIC.user_id
        CROSS JOIN 
        (
            SELECT count(*) as n_total_videos
            FROM `{latest_dataset}.video_axis`
        ) NV
        WHERE ((ROL.roles = 'Student') OR (ROL.roles is NULL))
    )
GROUP BY user_id, certified, verified, n_total_videos
order by user_id