{% set timeout_short = int(timeout_short) if timeout_short is defined and timeout_short else 5 %}
{% set timeout_long = int(timeout_long) if timeout_long is defined and timeout_long else 30 %}
SELECT 
  	"{{ course_id }}" as course_id,
    username, 
    sum(total_time_{{ timeout_short }}) as total_time_{{ timeout_short }},
    sum(total_time_{{ timeout_long }}) as total_time_{{ timeout_long }},
    sum(total_video_time_{{ timeout_short }}) as total_video_time_{{ timeout_short }},
    sum(total_video_time_{{ timeout_long }}) as total_video_time_{{ timeout_long }},
    sum(serial_video_time_{{ timeout_long }}) as serial_video_time_{{ timeout_long }},
    sum(total_problem_time_{{ timeout_short }}) as total_problem_time_{{ timeout_short }},
    sum(total_problem_time_{{ timeout_long }}) as total_problem_time_{{ timeout_long }},
    sum(serial_problem_time_{{ timeout_long }}) as serial_problem_time_{{ timeout_long }},
    sum(total_forum_time_{{ timeout_short }}) as total_forum_time_{{ timeout_short }},
    sum(total_forum_time_{{ timeout_long }}) as total_forum_time_{{ timeout_long }},
    sum(serial_forum_time_{{ timeout_long }}) as serial_forum_time_{{ timeout_long }},
    sum(total_text_time_{{ timeout_short }}) as total_text_time_{{ timeout_short }},
    sum(total_text_time_{{ timeout_long }}) as total_text_time_{{ timeout_long }},
    sum(serial_text_time_{{ timeout_long }}) as serial_text_time_{{ timeout_long }},
FROM `{{ latest_dataset }}.time_on_task`
GROUP BY course_id, username
order by username