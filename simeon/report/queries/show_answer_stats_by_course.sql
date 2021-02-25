SELECT
"{course_id}" as course_id,
avg(pct_show_answer_problem_seen) as avg_pct_show_answer_problem_seen,
avg(pct_show_answer_not_attempted) as avg_pct_show_answer_not_attempted,
avg(pct_show_answer_attempted) as avg_pct_show_answer_attempted,
avg(pct_show_answer_perfect) as avg_pct_show_answer_perfect,
avg(pct_show_answer_partial) as avg_pct_show_answer_partial,
avg(case when certified = "tbd" then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_certified, 
avg(case when certified = "tbd" then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_certified,
avg(case when certified = "tbd" then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_certified,
avg(case when certified = "tbd" then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_certified,
avg(case when certified = "tbd" then pct_show_answer_partial end) as avg_pct_show_answer_partial_certified,
avg(case when explored = "tbd" then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_explored,
avg(case when explored = "tbd"  then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_explored,
avg(case when explored = "tbd"  then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_explored,
avg(case when explored = "tbd"  then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_explored,
avg(case when explored = "tbd"  then pct_show_answer_partial end) as avg_pct_show_answer_partial_explored,
avg(case when verified = "tbd" then pct_show_answer_problem_seen end) as avg_pct_show_answer_problem_seen_verified,
avg(case when verified = "tbd" then pct_show_answer_not_attempted end) as avg_pct_show_answer_not_attempted_verified,
avg(case when verified = "tbd" then pct_show_answer_attempted end) as avg_pct_show_answer_attempted_verified,
avg(case when verified = "tbd" then pct_show_answer_perfect end) as avg_pct_show_answer_perfect_verified,
avg(case when verified = "tbd" then pct_show_answer_partial end) as avg_pct_show_answer_partial_verified,
max(case when has_pct_show_answer_problem_seen then median_pct_show_answer_problem_seen end) as median_pct_show_answer_problem_seen,
max(case when has_pct_show_answer_not_attempted then median_pct_show_answer_not_attempted end) as median_pct_show_answer_not_attempted,
max(case when has_pct_show_answer_attempted then median_pct_show_answer_attempted end) as median_pct_show_answer_attempted,
max(case when has_pct_show_answer_perfect then median_pct_show_answer_perfect end) as median_pct_show_answer_perfect,
max(case when has_pct_show_answer_partial then median_pct_show_answer_partial end) as median_pct_show_answer_partial,
max(case when has_pct_show_answer_problem_seen_explored then median_pct_show_answer_problem_seen_explored end) as median_pct_show_answer_problem_seen_explored,
max(case when has_pct_show_answer_not_attempted_explored then median_pct_show_answer_not_attempted_explored end) as median_pct_show_answer_not_attempted_explored,
max(case when has_pct_show_answer_attempted_explored then median_pct_show_answer_attempted_explored end) as median_pct_show_answer_attempted_explored,
max(case when has_pct_show_answer_perfect_explored then median_pct_show_answer_perfect_explored end) as median_pct_show_answer_perfect_explored,
max(case when has_pct_show_answer_partial_explored then median_pct_show_answer_partial_explored end) as median_pct_show_answer_partial_explored,
max(case when has_pct_show_answer_problem_seen_certified then median_pct_show_answer_problem_seen_certified end) as median_pct_show_answer_problem_seen_certified,
max(case when has_pct_show_answer_not_attempted_certified then median_pct_show_answer_not_attempted_certified end) as median_pct_show_answer_not_attempted_certified,
max(case when has_pct_show_answer_attempted_certified then median_pct_show_answer_attempted_certified end) as median_pct_show_answer_attempted_certified,
max(case when has_pct_show_answer_perfect_certified then median_pct_show_answer_perfect_certified end) as median_pct_show_answer_perfect_certified,
max(case when has_pct_show_answer_partial_certified then median_pct_show_answer_partial_certified end) as median_pct_show_answer_partial_certified,
max(case when has_pct_show_answer_problem_seen_verified then median_pct_show_answer_problem_seen_verified end) as median_pct_show_answer_problem_seen_verified,
max(case when has_pct_show_answer_not_attempted_verified then median_pct_show_answer_not_attempted_verified end) as median_pct_show_answer_not_attempted_verified,
max(case when has_pct_show_answer_attempted_verified then median_pct_show_answer_attempted_verified end) as median_pct_show_answer_attempted_verified,
max(case when has_pct_show_answer_perfect_verified then median_pct_show_answer_perfect_verified end) as median_pct_show_answer_perfect_verified,
max(case when has_pct_show_answer_partial_verified then median_pct_show_answer_partial_verified end) as median_pct_show_answer_partial_verified,
FROM
(
  SELECT *,
    (case when pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen,
    PERCENTILE_DISC(pct_show_answer_problem_seen, 0.5) over (partition by (case when pct_show_answer_problem_seen is not null then true end)) as median_pct_show_answer_problem_seen,
    (case when pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted,
    PERCENTILE_DISC(pct_show_answer_not_attempted ,0.5) over (partition by (case when pct_show_answer_not_attempted is not null then true end)) as median_pct_show_answer_not_attempted,
    (case when pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted,
    PERCENTILE_DISC(pct_show_answer_attempted ,0.5) over (partition by (case when pct_show_answer_attempted is not null then true end)) as median_pct_show_answer_attempted,
    (case when pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect,
    PERCENTILE_DISC(pct_show_answer_perfect ,0.5) over (partition by (case when pct_show_answer_perfect is not null then true end)) as median_pct_show_answer_perfect,
    (case when pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial,
    PERCENTILE_DISC(pct_show_answer_partial ,0.5) over (partition by (case when pct_show_answer_partial is not null then true end)) as median_pct_show_answer_partial,
    (case when explored =
"tbd"  and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_explored,
    PERCENTILE_DISC(pct_show_answer_problem_seen ,0.5) over (partition by (case when explored =
"tbd"  and pct_show_answer_problem_seen is not null then true end)) as median_pct_show_answer_problem_seen_explored,
    (case when explored =
"tbd"  and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_explored,
    PERCENTILE_DISC(pct_show_answer_not_attempted ,0.5) over (partition by (case when explored =
"tbd"  and  pct_show_answer_not_attempted is not null then true end)) as median_pct_show_answer_not_attempted_explored,
    (case when explored =
"tbd"  and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_explored,
    PERCENTILE_DISC(pct_show_answer_attempted ,0.5) over (partition by (case when explored =
"tbd"  and  pct_show_answer_attempted is not null then true end)) as median_pct_show_answer_attempted_explored,
    (case when explored =
"tbd"  and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_explored,
    PERCENTILE_DISC(pct_show_answer_perfect ,0.5) over (partition by (case when explored =
"tbd"  and  pct_show_answer_perfect is not null then true end)) as median_pct_show_answer_perfect_explored,
    (case when explored =
"tbd"  and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_explored,
    PERCENTILE_DISC(pct_show_answer_partial ,0.5) over (partition by (case when explored =
"tbd"  and  pct_show_answer_partial is not null then true end)) as median_pct_show_answer_partial_explored,
    (case when certified = "tbd" 
and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_certified,
    PERCENTILE_DISC(pct_show_answer_problem_seen ,0.5) over (partition by (case when certified = "tbd" 
and pct_show_answer_problem_seen is not null then true end)) as median_pct_show_answer_problem_seen_certified,
    (case when certified = "tbd" 
and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_certified,
    PERCENTILE_DISC(pct_show_answer_not_attempted ,0.5) over (partition by (case when certified = "tbd" 
and  pct_show_answer_not_attempted is not null then true end)) as median_pct_show_answer_not_attempted_certified,
    (case when certified = "tbd" 
and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_certified,
    PERCENTILE_DISC(pct_show_answer_attempted ,0.5) over (partition by (case when certified = "tbd" 
and  pct_show_answer_attempted is not null then true end)) as median_pct_show_answer_attempted_certified,
    (case when certified = "tbd" 
and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_certified,
    PERCENTILE_DISC(pct_show_answer_perfect ,0.5) over (partition by (case when certified = "tbd" 
and  pct_show_answer_perfect is not null then true end)) as median_pct_show_answer_perfect_certified,
    (case when certified = "tbd" 
and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_certified,
    PERCENTILE_DISC(pct_show_answer_partial ,0.5) over (partition by (case when certified = "tbd" 
and  pct_show_answer_partial is not null then true end)) as median_pct_show_answer_partial_certified,
    (case when verified = "tbd" and pct_show_answer_problem_seen is not null then true end) as has_pct_show_answer_problem_seen_verified,
    PERCENTILE_DISC(pct_show_answer_problem_seen ,0.5) over (partition by (case when verified = "tbd" and pct_show_answer_problem_seen is not null then true end)) as median_pct_show_answer_problem_seen_verified,
    (case when verified = "tbd" and  pct_show_answer_not_attempted is not null then true end) as has_pct_show_answer_not_attempted_verified,
    PERCENTILE_DISC(pct_show_answer_not_attempted ,0.5) over (partition by (case when verified = "tbd" and  pct_show_answer_not_attempted is not null then true end)) as median_pct_show_answer_not_attempted_verified,
    (case when verified = "tbd" and  pct_show_answer_attempted is not null then true end) as has_pct_show_answer_attempted_verified,
    PERCENTILE_DISC(pct_show_answer_attempted ,0.5) over (partition by (case when verified = "tbd" and  pct_show_answer_attempted is not null then true end)) as median_pct_show_answer_attempted_verified,
    (case when verified = "tbd" and  pct_show_answer_perfect is not null then true end) as has_pct_show_answer_perfect_verified,
    PERCENTILE_DISC(pct_show_answer_perfect ,0.5) over (partition by (case when verified = "tbd" and  pct_show_answer_perfect is not null then true end)) as median_pct_show_answer_perfect_verified,
    (case when verified = "tbd" and  pct_show_answer_partial is not null then true end) as has_pct_show_answer_partial_verified,
    PERCENTILE_DISC(pct_show_answer_partial ,0.5) over (partition by (case when verified = "tbd" and  pct_show_answer_partial is not null then true end)) as median_pct_show_answer_partial_verified,
  FROM `{latest_dataset}.show_answer_stats_by_user`)