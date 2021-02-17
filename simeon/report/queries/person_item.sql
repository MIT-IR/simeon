SELECT 
    user_id, 
    pa.course_id,
    CI.item_short_id as item_short_id,
    CI.item_nid as item_nid,
    item_grade,
    grade,
    n_attempts,
    date
FROM
(
    SELECT 
        user_id,
        course_id,
        item.answer_id as item_id,
        if(item.correct_bool, 1, 0) as item_grade,
	    grade,
        attempts as n_attempts,
        max(created) as date,
    FROM `{latest_dataset}.problem_analysis`
    CROSS JOIN UNNEST(item) as item
    group by 
        user_id, 
        course_id, 
        item_id, 
        item_grade, 
	    grade,
        n_attempts
) PA
JOIN `{latest_dataset}.course_item` CI
on PA.item_id = CI.item_id
order by user_id, CI.content_index, CI.item_number