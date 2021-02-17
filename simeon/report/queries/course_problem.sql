SELECT 
    course_id, 
    problem_nid, 
    problem_id, 
    problem_short_id, 
    avg(problem_grade) as avg_problem_raw_score,
    stddev(problem_grade) as sdv_problem_raw_score,
    max(possible_raw_score) as max_possible_raw_score,
    avg(problem_grade / possible_raw_score * 100) as avg_problem_pct_score,
    count(distinct(user_id)) as n_unique_users_attempted,
    problem_name,
    is_split,
    split_name,
FROM
(
    SELECT 
        CI.course_id, 
        problem_nid, 
        problem_id, 
        problem_short_id, 
        sum(item_grade) as problem_grade, 
        user_id,
        sum(CI.item_points_possible) as possible_raw_score, 
        problem_name, 
        is_split, 
        split_name
    FROM `{latest_dataset}.person_item` PI
    JOIN `{latest_dataset}.course_item` CI
    on PI.item_nid = CI.item_nid
    group by 
        course_id, 
        problem_nid, 
        problem_short_id, 
        problem_id, 
        user_id, 
        problem_name, 
        is_split, 
        split_name
)
group by 
    course_id, 
    problem_nid, 
    problem_id, 
    problem_short_id, 
    problem_name, 
    is_split, 
    split_name
order by avg_problem_pct_score desc