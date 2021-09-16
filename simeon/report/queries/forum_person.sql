-- FORUM_PERSON definition using the FORUM_EVENTS and FORUM_POSTS tables
SELECT
    IFNULL(PP.username_fp, FE.username_fe) as username,
    "{course_id}" as course_id,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.slug_id
    ELSE FE.slug_id END AS slug_id,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.slug_type
    ELSE FE.slug_type END AS slug_type,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.thread_id
    ELSE FE.thread_id END AS thread_id,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.parent_id
    ELSE FE.parent_id END AS parent_id,
    IFNULL(PP.original_poster, FE.original_poster) AS original_poster,
    IFNULL(PP.responded_to, FE.responded_to) AS responded_to,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.title
    ELSE FE.title END AS title,
    CASE WHEN PP.username_fp IS NOT NULL THEN PP.wrote
    ELSE 0 END AS wrote,
    FE.read as read,
    FE.pin as pinned,
    FE.upvote as upvoted,
    FE.unvote as unvoted,
    -- FE.del as deleted,
    FE.follow as followed,
    CASE WHEN PP.first_time IS NOT NULL
        AND FE.last_time IS NOT NULL
        AND PP.first_time <= FE.last_time
        THEN PP.first_time
    WHEN PP.first_time IS NOT NULL
        AND FE.last_time IS NULL
        THEN PP.first_time
    WHEN PP.first_time IS NOT NULL
        AND FE.last_time IS NULL
        THEN PP.first_time
    ELSE NULL END AS first_time,
    CASE WHEN PP.first_time IS NOT NULL
        AND FE.last_time IS NOT NULL
        AND PP.first_time >= FE.last_time
        THEN PP.first_time
    WHEN PP.first_time IS NOT NULL
        AND FE.last_time IS NULL
        THEN PP.first_time
    WHEN FE.last_time IS NOT NULL
        THEN FE.last_time
    ELSE FE.first_time END AS last_time
FROM (
    SELECT
        username as username_fp,
        slug_id,
        slug_type,
        thread_id,
        parent_id,
        original_poster,
        responded_to,
        title,
        1 as wrote,
        -- created_at as first_time,
        first_time
    FROM
        `{latest_dataset}.forum_posts`
    ORDER BY
        username_fp,
        first_time
) AS PP
FULL OUTER JOIN (
    SELECT
        DISTINCT
        username as username_fe,
        slug_id,
        thread_id,
        slug_type,
        original_poster,
        responded_to,
        title,
        FIRST_VALUE(time) OVER forum_window as first_time,
        LAST_VALUE(time) OVER forum_window as last_time,
        FIRST_VALUE(parent_id) OVER forum_window as parent_id,
        SUM(read) OVER forum_window as read,
        SUM(pin) OVER forum_window as pin,
        SUM(upvote) OVER forum_window as upvote,
        SUM(unvote) OVER forum_window as unvote,
        SUM(follow) OVER forum_window as follow
    FROM (
        SELECT
            FE.username as username,
            F.slug_id,
            FE.thread_id as thread_id,
            F.slug_type as slug_type,
            F.original_poster as original_poster,
            F.responded_to as responded_to,
            F.title as title,
            FE.time as time,
            F.parent_id as parent_id,
            CASE WHEN FE.forum_action = "read" OR FE.forum_action = "read_inline" THEN 1
            ELSE 0 END AS read,
            CASE WHEN FE.forum_action = "pin" THEN 1 ELSE 0 END AS pin,
            CASE WHEN FE.forum_action = "upvote" THEN 1 ELSE 0 END AS upvote,
            CASE WHEN FE.forum_action = "unvote" THEN 1 ELSE 0 END AS unvote,
            CASE WHEN FE.forum_action = "follow_thread" THEN 1 ELSE 0 END AS follow
        FROM `{latest_dataset}.forum_events` AS FE
        JOIN (
            SELECT
                username as username_fe,
                slug_id,
                slug_type,
                thread_id,
                parent_id,
                original_poster,
                responded_to,
                title,
                first_time
            FROM
                `{latest_dataset}.forum_posts`
        ) AS F ON F.thread_id = FE.thread_id
        WHERE FE.forum_action in (
            "read", "read_inline", "pin",
            "upvote", "unvote", "follow_thread"
        )
    )
    WINDOW forum_window AS (
        PARTITION BY username,
            slug_id,
            thread_id,
            slug_type,
            original_poster,
            responded_to,
            title
        ORDER BY time
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
) AS FE ON PP.username_fp = FE.username_fe
WHERE
    PP.username_fp IS NOT NULL AND PP.username_fp != ''
OR
    FE.username_fe IS NOT NULL AND FE.username_fe != ''
