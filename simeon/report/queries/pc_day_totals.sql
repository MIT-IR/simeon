select 
    username, 
    "{course_id}" as course_id,
    count(*) as ndays_act,
    sum(nevents) as nevents,
    sum(nprogcheck) as nprogcheck,
    sum(nshow_answer) as nshow_answer,
    sum(nvideo) as nvideo,
    sum(nproblem_check) as nproblem_check,
    sum(nforum) as nforum,
    sum(ntranscript) as ntranscript,
    sum(nseq_goto) as nseq_goto,
    sum(nseek_video) as nseek_video,
    sum(npause_video) as npause_video,
    MIN(first_event) as first_event,
    MAX(last_event) as last_event,
    AVG(avg_dt) as avg_dt,
    sqrt(sum(sdv_dt*sdv_dt * n_dt)/sum(n_dt)) as sdv_dt,
    MAX(max_dt) as max_dt,
    sum(n_dt) as n_dt,
    sum(sum_dt) as sum_dt
from
    `{latest_dataset}.person_course_day`
group by username
order by sum_dt desc