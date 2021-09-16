SELECT 
    "{{ course_id }}" as course_id,
    module_id,
    COUNT(DISTINCT username) as n_unique_users,
    COUNT(DISTINCT certified_username) as n_unique_certified,
    AVG(time_umid5) as mean_tmid5,
    AVG(cert_time_umid5) as cert_mean_tmid5,
    AVG(time_umid30) as mean_tmid30,
    AVG(cert_time_umid30) as cert_mean_tmid30,
    MAX(median_tmid5) as median_tmid5,
    MAX(cert_median_tmid5) as cert_median_tmid5,
    MAX(median_tmid30) as median_tmid30,
    MAX(cert_median_tmid30) as cert_median_tmid30,
    SUM(time_umid5) as total_tmid5,   -- total time on module (by module_id) in seconds
    SUM(cert_time_umid5) as cert_total_tmid5,
    SUM(time_umid30) as total_tmid30, -- mid5 has 5 minute timeout, mid30 has 30 min timeout
    SUM(cert_time_umid30) as cert_total_tmid30,
FROM (
    SELECT
        module_id as module_id,
        username as username,
        (case when certified then username else null end) as certified_username,
        SUM(time_umid5) as time_umid5,
        SUM(time_umid30) as time_umid30,
        SUM(case when certified then time_umid5 else null end) as cert_time_umid5,
        SUM(case when certified then time_umid30 else null end) as cert_time_umid30,
        MAX(median_tmid5) as median_tmid5,
        MAX(cert_median_tmid5) as cert_median_tmid5,
        MAX(median_tmid30) as median_tmid30,
        MAX(cert_median_tmid30) as cert_median_tmid30,
    FROM (
        SELECT module_id,
            TOA.username,
            PC.certified,
            TOA.time_umid5,
            TOA.time_umid30,
            PERCENTILE_CONT(TOA.time_umid5, 0.5) over(partition by module_id) as median_tmid5,
            PERCENTILE_CONT(TOA.time_umid30, 0.5) over(partition by module_id) as median_tmid30,
            PERCENTILE_CONT(case when PC.certified then TOA.time_umid5 else null end, 0.5) over(partition by module_id) as cert_median_tmid5,
            PERCENTILE_CONT(case when PC.certified then TOA.time_umid30 else null end, 0.5) over(partition by module_id) as cert_median_tmid30
        FROM `{{ latest_dataset }}.time_on_asset_daily` TOA
        JOIN `{{ latest_dataset }}.person_course` PC  -- join to know who certified or attempted a problem
        ON TOA.username = PC.username
        WHERE TOA.time_umid5 is not null
            AND PC.nproblem_check > 0
    )
    GROUP BY module_id, username, certified_username
    ORDER BY module_id, username
)
GROUP BY module_id
order by module_id