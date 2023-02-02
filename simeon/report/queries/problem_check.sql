SELECT 
    time, 
    username,
    "{{ course_id }}"  as course_id,
    module_id,
    event_struct.answers as student_answers,
    event_struct.attempts as attempts,
    event_struct.success as success,
    event_struct.grade as grade,
from `{{log_dataset}}.tracklog_*`
where (event_type = "problem_check" or event_type = "save_problem_check")
and event_source = "server"
order by time