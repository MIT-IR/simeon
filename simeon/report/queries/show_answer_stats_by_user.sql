SELECT
  user_id,
  explored,
  certified,
  verified,
  safe_divide(n_show_answer_problem_seen, n_problems_seen) * 100 as pct_show_answer_problem_seen,
  safe_divide(n_show_answer_not_attempted, n_not_attempted) * 100 as pct_show_answer_not_attempted,
  safe_divide(n_show_answer_attempted,n_attempted) * 100  as pct_show_answer_attempted,
  safe_divide(n_show_answer_perfect, n_perfect) * 100 as pct_show_answer_perfect,
  safe_divide(n_show_answer_partial,n_partial) * 100 as pct_show_answer_partial,
  n_show_answer_problem_seen,
  n_problems_seen,
  n_show_answer_not_attempted,
  n_not_attempted,
  n_show_answer_attempted,
  n_attempted,
  n_show_answer_perfect,
  n_perfect,
  n_show_answer_partial,
  n_partial,
FROM
(
    SELECT
        A.user_id as user_id,
        cast(null as bool) as explored, -- replace when person_course is filled in
        cast(null as bool) as certified, -- replace when person_course is filled in
        cast(null as bool) as verified, -- replace when person_course is filled in
        A.n_show_answer_not_attempted as n_show_answer_not_attempted,
        A.n_not_attempted as n_not_attempted,
        A.n_show_answer_attempted as n_show_answer_attempted,
        A.n_attempted as n_attempted,
        A.n_show_answer_perfect as n_show_answer_perfect,
        A.n_perfect as n_perfect,
        A.n_show_answer_partial as n_show_answer_partial,
        A.n_partial as n_partial,
        A.n_show_answer_problem_seen as n_show_answer_problem_seen,
        A.n_problems_seen as n_problems_seen,
        
    FROM
    (
        SELECT 
          PG.user_id as user_id,
          sum(case when (not PG.attempted) and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_not_attempted,
          sum(case when (not PG.attempted) then 1 else 0 end) as n_not_attempted,
          sum(case when PG.attempted and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_attempted,
          sum(case when PG.attempted then 1 else 0 end) as n_attempted,
          sum(case when PG.perfect and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_perfect,
          sum(case when PG.perfect then 1 else 0 end) as n_perfect,
          sum(case when (PG.grade > 0) and (n_show_answer > 0) then 1 else 0 end) as n_show_answer_partial,
          sum(case when PG.grade > 0 then 1 else 0 end) as n_partial,
          sum(case when n_show_answer > 0 then 1 else 0 end) as n_show_answer_problem_seen,
          count(*) as n_problems_seen,
        FROM 
          `{latest_dataset}.problem_grades` as PG
        LEFT JOIN 
        (
            SELECT 
              SA.module_id as module_id,  
              UIC.user_id,
              count(*) as n_show_answer,
            FROM `{latest_dataset}.show_answer`  SA
            join `{latest_dataset}.user_info_combo` UIC using(username)
            group by module_id, user_id
            order by user_id
        ) as SA
        ON SA.user_id = PG.user_id
           AND SA.module_id = PG.module_id
        group by user_id
     ) as A
)
ORDER BY user_id