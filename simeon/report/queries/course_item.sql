SELECT 
    '{course_id}' as course_id,
    *,
    CONCAT(assignment_short_id, "__", cast(problem_number as string)) as problem_short_id,
    CONCAT(assignment_short_id, "__", cast(problem_number as string), "_", cast(item_number as string)) as item_short_id,
    row_number() over (order by content_index, item_number) as item_nid,
    sum(item_weight) over (order by content_index, item_number) cumulative_item_weight
FROM
    (
        SELECT 
            item_id, 
            problem_id,
            max(if(item_number=1, x_item_nid, null)) over (partition by problem_id) as problem_nid,
            CONCAT(
                        IFNULL(GP.short_label, ""),
                        "_", cast(assignment_seq_num as string)
                    ) as assignment_short_id,
            problem_weight * 
            (
                safe_divide(
                    safe_divide(
                        safe_divide(IFNULL(GP.fraction_of_overall_grade, 1.0), n_items),
                        sum_problem_weight_in_assignment
                    ),
                    n_assignments_of_type
                )
            ) as item_weight,
            n_user_responses,
            chapter_name,
            section_name,
            vertical_name,
            problem_name,
            CI.assignment_id as assignment_id,
            n_problems_in_assignment,
            CI.assignment_type as assignment_type,
            IFNULL(GP.fraction_of_overall_grade, 1.0) as assignment_type_weight,
            n_assignments_of_type,
            assignment_seq_num,
            chapter_number,
            content_index,
            section_number,
            problem_number,
            problem_weight,
            item_points_possible,
            problem_points_possible,
            emperical_item_points_possible,
            emperical_problem_points_possible,
            item_number,
            n_items,
            start_date,
            due_date,
            is_split,
            split_name,
            problem_path
        FROM
            (
                SELECT 
                    item_id, 
                    item_number,
                    n_items,
                    problem_id,
                    row_number() over (partition by item_number order by content_index) as x_item_nid,
                    n_user_responses,
                    chapter_name,
                    section_name,
                    vertical_name,
                    problem_name,
                    assignment_id,
                    sum(if(assignment_id is not null and item_number=1, 1, 0)) over (partition by assignment_id) n_problems_in_assignment,
                    sum(if(assignment_id is not null and item_number=1, problem_weight, 0)) 
                        over (partition by assignment_id) sum_problem_weight_in_assignment,
                    assignment_type,
                    n_assignments_of_type,
                    assignment_seq_num,
                    chapter_number,
                    section_number,
                    problem_number,
                    problem_path,
                    content_index,
                    start_date,
                    due_date,
                    is_split,
                    split_name,
                    problem_weight,
                    item_points_possible,
                    problem_points_possible,
                    emperical_item_points_possible,
                    emperical_problem_points_possible
                FROM
                    (
                        SELECT 
                            item_id, 
                            item_number,
                            n_items,
                            problem_id,
                            n_user_responses,
                            CA.name as problem_name,
                            chapter_name,
                            section_name,
                            vertical_name,
                            assignment_id,
                            assignment_type,
                            n_assignments_of_type,
                            CA.assignment_seq_num as assignment_seq_num,
                            CA.chapter_number as chapter_number,
                            CA.section_number as section_number,
                            CA.problem_number as problem_number,
                            CA.path as problem_path,
                            CA.index as content_index,
                            CA.start as start_date,
                            CA.due as due_date,
                            CA.is_split as is_split,
                            CA.split_name as split_name,
                            IFNULL(CA.weight, 1.0) as problem_weight,
                            item_points_possible,
                            problem_points_possible,
                            emperical_item_points_possible,
                            emperical_problem_points_possible
                        FROM
                            (
                                SELECT 
                                    item_id, 
                                    item_number,
                                    n_items,
                                    problem_id,
                                    n_user_responses,
                                    1.0 as item_points_possible,
                                    cast(n_items as float64) as problem_points_possible,
                                    safe_divide(problem_points_possible, n_items) as emperical_item_points_possible,
                                    problem_points_possible as emperical_problem_points_possible,
                                FROM
                                    (
                                        SELECT 
                                            item_id, 
                                            item_number,
                                            max(item_number) over (partition by problem_id) n_items,
                                            problem_id,
                                            problem_points_possible,
                                            n_user_responses
                                        FROM
                                            (
                                                SELECT 
                                                    item_id,
                                                    row_number() over (partition by problem_id order by item_id) item_number,
                                                    problem_id,
                                                    problem_points_possible,
                                                    n_user_responses 
                                                FROM
                                                    (
                                                        SELECT 
                                                            item.answer_id as item_id,
                                                            problem_url_name as problem_id,
                                                            max_grade as problem_points_possible,
                                                            count(*) as n_user_responses
                                                        FROM `{latest_dataset}.problem_analysis`
                                                        CROSS JOIN UNNEST(item) as item
                                                        GROUP BY 
                                                            item_id, 
                                                            problem_id, 
                                                            problem_points_possible
                                                        HAVING n_user_responses > 5
                                                    )
                                            )
                                    ) 
                                ORDER BY item_id, item_number
                            ) PA
                        JOIN 
                            (
                                SELECT 
                                    module_id,
                                    url_name,
                                    index,
                                    weight,
                                    assignment_type,
                                    MAX(IF(problem_number=1, x_assignment_seq_num, null)) over (partition by assignment_id) as assignment_seq_num,
                                    problem_number,
                                    assignment_id,
                                    n_assignments_of_type,
                                    chapter_name,
                                    section_name,
                                    vertical_name,
                                    name,
                                    path,
                                    start,
                                    due,
                                    is_split,
                                    split_name,
                                    chapter_number,
                                    section_number
                                FROM 
                                    (
                                        SELECT 
                                            *,  
                                            SUM(IF(problem_number=1, 1, 0)) over (partition by assignment_type) n_assignments_of_type,
                                            row_number() over (partition by assignment_type, problem_number order by index) as x_assignment_seq_num
                                        FROM
                                            (
                                                SELECT 
                                                    module_id,
                                                    url_name,
                                                    index,
                                                    weight,
                                                    assignment_type,
                                                    chapter_number,
                                                    section_number,
                                                    assignment_id,  
                                                    chapter_name,
                                                    section_name,
                                                    vertical_name,
                                                    name,
                                                    path,
                                                    start,
                                                    due,
                                                    is_split,
                                                    split_name,
                                                    row_number() over (partition by assignment_id order by index) problem_number
                                                FROM
                                                    (
                                                        SELECT 
                                                            CAI.module_id as module_id,
                                                            CAI.url_name as url_name,
                                                            index,
                                                            weight,
                                                            assignment_type,
                                                            chapter_number,
                                                            section_number,
                                                            --  assignment_id = assignment_type + ch_chapter_number + sec_section_number
                                                            CONCAT(assignment_type, "_ch", cast(chapter_number as string), "_sec", cast(section_number as string)) as assignment_id,  
                                                            chapter_name,
                                                            section_name,
                                                            name,
                                                            path,
                                                            start,
                                                            due,
                                                            is_split,
                                                            split_name,
                                                            parent
                                                        FROM 
                                                            (
                                                                SELECT
                                                                    module_id,
                                                                    url_name,
                                                                    index,
                                                                    IFNULL(data.weight, 1.0) as weight,
                                                                    ifnull(gformat, "") as assignment_type,
                                                                    chapter_mid as chapter_mid,
                                                                    REGEXP_EXTRACT(path, '^/[^/]+/([^/]+)') as section_mid,
                                                                    name,
                                                                    path,
                                                                    start,
                                                                    due,
                                                                    is_split,
                                                                    split_url_name as split_name,
                                                                    parent
                                                                FROM `{latest_dataset}.course_axis` CAI
                                                                WHERE 
                                                                    category = "problem"
                                                                ORDER BY index
                                                            ) CAI
                                                        LEFT JOIN
                                                            (
                                                                SELECT 
                                                                    module_id, 
                                                                    url_name, 
                                                                    name as section_name,
                                                                    max(if(category="chapter", x_chapter_number, null)) 
                                                                        over (partition by chapter_mid order by index) as chapter_number,
                                                                    section_number,
                                                                    chapter_name
                                                                FROM
                                                                    (
                                                                        SELECT 
                                                                            module_id, 
                                                                            url_name,
                                                                            row_number() over (partition by category order by index) as x_chapter_number,
                                                                            row_number() over (partition by chapter_mid, category order by index) as section_number,
                                                                            FIRST_VALUE(name) over (partition by chapter_mid order by index) as chapter_name,
                                                                            index,
                                                                            category,
                                                                            name,
                                                                            if(category="chapter", module_id, chapter_mid) as chapter_mid
                                                                        FROM  `{latest_dataset}.course_axis`
                                                                        WHERE 
                                                                            category = "chapter" or category = "sequential" or category = "videosequence"
                                                                        ORDER BY index
                                                                    )
                                                                ORDER BY index
                                                            ) CHN on CAI.section_mid = CHN.url_name
                                                    ) CAPN
                                                left join 
                                                    (
                                                        SELECT 
                                                            url_name as vertical_url_name, 
                                                            name as vertical_name,
                                                        FROM  `{latest_dataset}.course_axis`
                                                        WHERE 
                                                            category = "vertical"
                                                    ) CAV
                                                ON CAPN.parent = CAV.vertical_url_name
                                            )
                                    )
                            ) CA on PA.problem_id = CA.url_name
                    )
            ) CI
        LEFT JOIN 
            `{latest_dataset}.grading_policy` GP
        ON CI.assignment_type = GP.assignment_type
        ORDER BY content_index, item_number
    )
ORDER BY content_index, item_number