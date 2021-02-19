-- FORUM_POSTS definition using the FORUM table (which itself is made from an edX SQL file)
WITH FORUM_TEMP AS (
    SELECT
        _type,
        mongoid as slug_id,
        author_username as username,
        "{course_id}" as course_id,
        case when _type = "CommentThread"
            and comment_thread_id is null
            and parent_id is null
            and mongoid is not null then "initial_post"
        when _type = "Comment"
            and comment_thread_id is not null
            and parent_id is null
            and mongoid is not null then "response_post"
        when _type = "Comment"
            and parent_id is not null then "comment"
        else null
        end as slug_type,
        comment_thread_id as thread_id,
        parent_id,
        title,
        created_at as first_time,
        SUBSTR(body, 0, 100) as body_preview
    FROM `{latest_dataset}.forum`
)
    SELECT
        ADDED_TITLE.username as username,
        ADDED_TITLE.course_id as course_id,
        ADDED_TITLE.slug_id as slug_id,
        ADDED_TITLE.slug_type as slug_type,
        ADDED_TITLE.thread_id as thread_id,
        ADDED_TITLE.parent_id as parent_id,
        ADDED_TITLE.original_poster as original_poster,
        ADD_RESPOND_TO.username as responded_to,
        ADDED_TITLE.title as title,
        ADDED_TITLE.first_time as first_time,
        ADDED_TITLE.body_preview as body_preview
    FROM (
        SELECT
            IP.username as username,
            FA.course_id as course_id,
            FA.slug_id as slug_id,
            FA.slug_type as slug_type,
            FA.thread_id as thread_id,
            FA.parent_id as parent_id,
            IP.username as original_poster,
            FA.username as responded_to,
            IP.title as title,
            FA.first_time as first_time,
            FA.body_preview as body_preview
        FROM (
            SELECT * FROM FORUM_TEMP
            WHERE _type = "Comment" AND parent_id IS NOT NULL
        ) FA
        LEFT JOIN (
            SELECT username, title, slug_id FROM FORUM_TEMP
            WHERE _type = "CommentThread"
                AND thread_id is null
                AND parent_id is null
                AND slug_id is not null
        ) IP
        ON FA.thread_id = IP.slug_id
    ) AS ADDED_TITLE
    LEFT JOIN FORUM_TEMP AS ADD_RESPOND_TO
    ON ADDED_TITLE.parent_id = ADD_RESPOND_TO.slug_id
    UNION ALL
    SELECT
        ADDED_TITLE.username as username,
        ADDED_TITLE.course_id as course_id,
        ADDED_TITLE.slug_id as slug_id,
        ADDED_TITLE.slug_type as slug_type,
        ADDED_TITLE.thread_id as thread_id,
        ADDED_TITLE.parent_id as parent_id,
        ADDED_TITLE.original_poster as original_poster,
        ADD_RESPOND_TO.username as responded_to,
        ADDED_TITLE.title as title,
        ADDED_TITLE.first_time as first_time,
        ADDED_TITLE.body_preview as body_preview
    FROM (
        SELECT
            IP.username as username,
            FA.course_id as course_id,
            FA.slug_id as slug_id,
            FA.slug_type as slug_type,
            FA.thread_id as thread_id,
            FA.parent_id as parent_id,
            IP.username as original_poster,
            FA.username as responded_to,
            IP.title as title,
            FA.first_time as first_time,
            FA.body_preview as body_preview
        FROM (
            SELECT * FROM FORUM_TEMP
            WHERE _type = "Comment"
            AND thread_id is not null
            AND parent_id is null
            AND slug_id is not null
        ) FA
        LEFT JOIN (
            SELECT username, title, slug_id FROM FORUM_TEMP
            WHERE _type = "CommentThread"
                AND thread_id is null
                AND parent_id is null
                AND slug_id is not null
        ) IP
        ON FA.thread_id = IP.slug_id
    ) AS ADDED_TITLE
    LEFT JOIN FORUM_TEMP AS ADD_RESPOND_TO
    ON ADDED_TITLE.parent_id = ADD_RESPOND_TO.slug_id
    UNION ALL
    SELECT
        IP.username as username,
        FA.course_id as course_id,
        FA.slug_id as slug_id,
        FA.slug_type as slug_type,
        FA.thread_id as thread_id,
        FA.parent_id as parent_id,
        IP.username as original_poster,
        FA.username as responded_to,
        IP.title as title,
        FA.first_time as first_time,
        FA.body_preview as body_preview
    FROM (
        SELECT * FROM FORUM_TEMP
        WHERE _type = "Comment"
        AND thread_id is not null
        AND parent_id is null
        AND slug_id is not null
    ) FA
    LEFT JOIN (
        SELECT * FROM FORUM_TEMP
        WHERE _type = "CommentThread"
            AND thread_id is null
            AND parent_id is null
            AND slug_id is not null
    ) IP
    ON FA.thread_id = IP.slug_id
    UNION ALL
    SELECT
        username,
        course_id,
        slug_id,
        slug_type,
        thread_id,
        parent_id,
        username as original_poster,
        null as responded_to,
        title,
        first_time,
        body_preview
    FROM FORUM_TEMP
    WHERE _type = "CommentThread"
        AND thread_id is null
        AND parent_id is null
        AND slug_id is not null
