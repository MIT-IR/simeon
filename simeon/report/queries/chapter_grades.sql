SELECT
    *,
    PERCENTILE_DISC(chgrade, 0.5) OVER (PARTITION BY chapter_mid) as median_grade,
    NTH_VALUE(user_chapter_max_grade, 1)
        OVER (PARTITION BY chapter_mid ORDER BY user_chapter_max_grade DESC) as chmax,
FROM
    (
        SELECT
            user_id,
            chapter_mid,
            sum(max_grade) as user_chapter_max_grade,
            sum(grade) as chgrade,
            max(due_date) as due_date_max,
            min(due_date) as due_date_min,
        FROM
            (
                SELECT 
                    PG.user_id as user_id,
                    PG.module_id as module_id,
                    PG.grade as grade,
                    PG.max_grade as max_grade,
                    CA.name as name,
                    CA.gformat as gformat,
                    CA.chapter_mid as chapter_mid,
                    CA.due as due_date,
                FROM `{latest_dataset}.problem_grades` PG
                JOIN `{latest_dataset}.course_axis`  CA
                    ON CA.module_id = PG.module_id
                WHERE PG.grade is not null
                ORDER BY due_date
            )
        GROUP BY user_id, chapter_mid
        ORDER BY user_id
    )
ORDER BY user_id