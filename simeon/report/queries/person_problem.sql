SELECT 
    user_id,
    PI.course_id,
    CI.problem_nid as problem_nid,
    sum(item_grade) as problem_raw_score,
    sum(item_grade) / sum(CI.item_points_possible) * 100 as problem_pct_score,
    max(PI.grade) as grade,
    max(n_attempts) as n_attempts,
    max(date) as date,
FROM `{latest_dataset}.person_item` PI
JOIN `{latest_dataset}.course_item` CI
on PI.item_nid = CI.item_nid
group by user_id, course_id, problem_nid
order by user_id, course_id, problem_nid