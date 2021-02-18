SELECT 
    username,
    '{course_id}' AS course_id,
    DATE(time) AS date,
    SUM(bevent) AS nevents,
    SUM(bprogress) AS nprogcheck,
    SUM(bshow_answer) AS nshow_answer,
    SUM(bvideo) AS nvideo,
    SUM(bproblem_check) AS nproblem_check,
    SUM(bforum) AS nforum,
    SUM(bshow_transcript) AS ntranscript,
    SUM(bseq_goto) AS nseq_goto,
    SUM(bseek_video) AS nseek_video,
    SUM(bpause_video) AS npause_video,
    COUNT(DISTINCT video_id) AS nvideos_viewed, # New Video - Unique videos viewed
	SUM(case when position is not null then cast(position as float64) else cast(0.0 as float64) end) AS nvideos_watched_sec, # New Video - # sec watched using max video position
    SUM(case when read is not null then read else 0 end) AS nforum_reads, # New discussion - Forum reads
    SUM(case when write is not null then write else 0 end) AS nforum_posts, # New discussion - Forum posts
    COUNT(DISTINCT thread_id ) AS nforum_threads, # New discussion - Unique forum threads interacted with
    COUNT(DISTINCT case when problem_nid != 0 then problem_nid else null end) AS nproblems_answered, # New Problem - Unique problems attempted
    SUM(case when n_attempts is not null then n_attempts else 0 end) AS nproblems_attempted, # New Problem - Total attempts
    SUM(case when ncount_problem_multiplechoice is not null then ncount_problem_multiplechoice else 0 end) as nproblems_multiplechoice,
    SUM(case when ncount_problem_choice is not null then ncount_problem_choice else 0 end) as nproblems_choice,
    SUM(case when ncount_problem_numerical is not null then ncount_problem_numerical else 0 end) as nproblems_numerical,
    SUM(case when ncount_problem_option is not null then ncount_problem_option else 0 end) as nproblems_option,
    SUM(case when ncount_problem_custom is not null then ncount_problem_custom else 0 end) as nproblems_custom,
    SUM(case when ncount_problem_string is not null then ncount_problem_string else 0 end) as nproblems_string,
    SUM(case when ncount_problem_mixed is not null then ncount_problem_mixed else 0 end) as nproblems_mixed,
    SUM(case when ncount_problem_formula is not null then ncount_problem_formula else 0 end) as nproblems_forumula,
    SUM(case when ncount_problem_other is not null then ncount_problem_other else 0 end) as nproblems_other,
    MIN(time) AS first_event,
    MAX(time) AS last_event,
    AVG( CASE WHEN (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 > 5*60 THEN NULL ELSE (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 END ) AS avg_dt,
    STDDEV( CASE WHEN (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 > 5*60 THEN NULL ELSE (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 END ) AS sdv_dt,
    MAX( CASE WHEN (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 > 5*60 THEN NULL ELSE (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 END ) AS max_dt,
    COUNT( CASE WHEN (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 > 5*60 THEN NULL ELSE (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 END ) AS n_dt,
    SUM( CASE WHEN (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 > 5*60 THEN NULL ELSE (UNIX_MICROS(time) - unix_micros(last_time))/1.0E6 END ) AS sum_dt
FROM (
        SELECT
            *
        FROM 
            (
                SELECT
                    time,
                    '{course_id}' as course_id,
                    username,
                    CASE WHEN event_type = "play_video" THEN 1 ELSE 0 END AS bvideo,
                    CASE WHEN event_type = "problem_check" THEN 1 ELSE 0 END AS bproblem_check,
                    CASE WHEN username != "" THEN 1 ELSE 0 END AS bevent,
                    CASE WHEN REGEXP_CONTAINS(event_type, "^/courses/{course_id}/discussion/.*") then 1 else 0 end as bforum,
                    CASE WHEN REGEXP_CONTAINS(event_type, "^/courses/{course_id}/progress") then 1 else 0 end as bprogress,
                    CASE WHEN event_type IN ("show_answer", "showanswer") THEN 1 ELSE 0 END AS bshow_answer,
                    CASE WHEN event_type = 'show_transcript' THEN 1 ELSE 0 END AS bshow_transcript,
                    CASE WHEN event_type = 'seq_goto' THEN 1 ELSE 0 END AS bseq_goto,
                    CASE WHEN event_type = 'seek_video' THEN 1 ELSE 0 END AS bseek_video,
                    CASE WHEN event_type = 'pause_video' THEN 1 ELSE 0 END AS bpause_video,
                    LAG(time, 1) OVER (PARTITION BY username ORDER BY time) last_time,
                    "" as video_id,
                    0 as position,
                    "" as thread_id,
                    0 as write,
                    0 as read,
                    0 as problem_nid,
                    0 as n_attempts,
                    0 as ncount_problem_multiplechoice,
                    0 as ncount_problem_choice,
                    0 as ncount_problem_numerical,
                    0 as ncount_problem_option,
                    0 as ncount_problem_custom,
                    0 as ncount_problem_string,
                    0 as ncount_problem_mixed,
                    0 as ncount_problem_formula,
                    0 as ncount_problem_other
                FROM `{log_dataset}.tracklog_*`   
                WHERE
                    NOT event_type like "%/xblock/%"
                    AND username != "" 
            ) UNION ALL
            ( # Video events
                SELECT 
                    TIMESTAMP(date) as time,
                    '{course_id}' as course_id,
                    username,
                    0 as bvideo,
                    0 as bproblem_check,
                    0 as bevent,
                    0 as bforum,
                    0 as bprogress,
                    0 as bshow_answer,
                    0 as bshow_transcript,
                    0 as bseq_goto,
                    0 as bseek_video,
                    0 as bpause_video,
                    TIMESTAMP(date) as last_time,
                    video_id,
                    position,
                    "" as thread_id,
                    0 as write,
                    0 as read,
                    0 as problem_nid,
                    0 as n_attempts,
                    0 as ncount_problem_multiplechoice,
                    0 as ncount_problem_choice,
                    0 as ncount_problem_numerical,
                    0 as ncount_problem_option,
                    0 as ncount_problem_custom,
                    0 as ncount_problem_string,
                    0 as ncount_problem_mixed,
                    0 as ncount_problem_formula,
                    0 as ncount_problem_other
                FROM `{latest_dataset}.video_stats_day`
            )UNION ALL
            ( # Forum Events
                SELECT 
                    time,
                    '{course_id}' as course_id,
                    username,
                    0 as bvideo,
                    0 as bproblem_check,
                    0 as bevent,
                    0 as bforum,
                    0 as bprogress,
                    0 as bshow_answer,
                    0 as bshow_transcript,
                    0 as bseq_goto,
                    0 as bseek_video,
                    0 as bpause_video,
                    time as last_time,
                    "" as video_id,
                    0 as position,
                    thread_id,
                    CASE 
                        WHEN (forum_action = "reply" or forum_action = "comment_reply"
                              or forum_action = "created_thread" or forum_action = "created_response" or forum_action = "created_comment") THEN 1 
                        ELSE 0 
                    END AS write,
                    CASE 
                        WHEN (forum_action = "read" or forum_action = "read_inline") THEN 1 
                        ELSE 0 
                    END AS read,
                    0 as problem_nid,
                    0 as n_attempts,
                    0 as ncount_problem_multiplechoice,
                    0 as ncount_problem_choice,
                    0 as ncount_problem_numerical,
                    0 as ncount_problem_option,
                    0 as ncount_problem_custom,
                    0 as ncount_problem_string,
                    0 as ncount_problem_mixed,
                    0 as ncount_problem_formula,
                    0 as ncount_problem_other
                FROM `{latest_dataset}.forum_events`
                WHERE (forum_action = "reply" or forum_action = "comment_reply"
                    or forum_action = "created_thread" or forum_action = "created_response" or forum_action = "created_comment"
                    or forum_action = "read" or forum_action = "read_inline")
            ) UNION ALL
			( # Problems
			    SELECT 
                    pp.time AS time,
				    '{course_id}' as course_id,
                    uic.username AS username,
                    0 as bvideo,
                    0 as bproblem_check,
                    0 as bevent,
                    0 as bforum,
                    0 as bprogress,
                    0 as bshow_answer,
                    0 as bshow_transcript,
                    0 as bseq_goto,
                    0 as bseek_video,
                    0 as bpause_video,
                    time as last_time,
                    "" as video_id,
                    0 as position,
                    "" as thread_id,
                    0 as write,
                    0 as read,
				    pp.problem_nid AS problem_nid,
				    pp.n_attempts AS n_attempts,
                    pp.ncount_problem_multiplechoice as ncount_problem_multiplechoice,
                    pp.ncount_problem_choice as ncount_problem_choice,
                    pp.ncount_problem_numerical as ncount_problem_numerical,
                    pp.ncount_problem_option as ncount_problem_option,
                    pp.ncount_problem_custom as ncount_problem_custom,
                    pp.ncount_problem_string as ncount_problem_string,
                    pp.ncount_problem_mixed as ncount_problem_mixed,
                    pp.ncount_problem_formula as ncount_problem_formula,
                    pp.ncount_problem_other as ncount_problem_other,
				   FROM (
					   (
					      SELECT PP.user_id as user_id,
						     PP.problem_nid AS problem_nid,
						     PP.n_attempts as n_attempts,
						     PP.date as time,
						     (Case when CP_CA.data_itype = "multiplechoiceresponse" then 1 else 0 end) as ncount_problem_multiplechoice, # Choice
					             (Case when CP_CA.data_itype = "choiceresponse" then 1 else 0 end) as ncount_problem_choice,       # Choice
						     (Case when CP_CA.data_itype = "numericalresponse" then 1 else 0 end) as ncount_problem_numerical, #input
						     (Case when CP_CA.data_itype = "optionresponse" then 1 else 0 end) as ncount_problem_option,       # Choice
					             (Case when CP_CA.data_itype = "customresponse" then 1 else 0 end) as ncount_problem_custom,       # Custom
					             (Case when CP_CA.data_itype = "stringresponse" then 1 else 0 end) as ncount_problem_string,       # Input
					             (Case when CP_CA.data_itype = "mixed" then 1 else 0 end) as ncount_problem_mixed,                 # Mixed
					             (Case when CP_CA.data_itype = "forumula" then 1 else 0 end) as ncount_problem_formula,            # Input
					             (Case when CP_CA.data_itype != "multiplechoiceresponse" and
							        CP_CA.data_itype != "choiceresponse" and
							        CP_CA.data_itype != "numericalresponse" and
							        CP_CA.data_itype != "optionresponse" and
							        CP_CA.data_itype != "customresponse" and
							        CP_CA.data_itype != "stringresponse" and
							        CP_CA.data_itype != "mixed" and
							        CP_CA.data_itype != "forumula"
							   then 1 else 0 end) as ncount_problem_other, # Input
						     #MAX(n_attempts) AS n_attempts,
						     #MAX(date) AS time,
					      FROM `{latest_dataset}.person_problem` PP
					      LEFT JOIN
					      (
							SELECT CP.problem_nid as problem_nid,
							       SAFE_CAST(CP.problem_id as INT64) as problem_id,
							       CA.data.itype as data_itype,
						        FROM `{latest_dataset}.course_problem` CP
						        LEFT JOIN `{latest_dataset}.course_axis` CA
						        ON CP.problem_id = CA.url_name
					      ) as CP_CA
					      ON PP.problem_nid = CP_CA.problem_nid
					      GROUP BY time, user_id, problem_nid, n_attempts,
						       ncount_problem_multiplechoice,
						       ncount_problem_choice,
						       ncount_problem_choice,
						       ncount_problem_numerical,
						       ncount_problem_option,
						       ncount_problem_custom,
						       ncount_problem_string,
						       ncount_problem_mixed,
						       ncount_problem_formula,
						       ncount_problem_other
					      )
					) AS pp
				        LEFT JOIN (
							      SELECT username,
								     user_id
							      FROM `{latest_dataset}.user_info_combo`
					) AS uic
					ON uic.user_id = pp.user_id
			  )
              )
			  GROUP BY course_id,
				   username,
				   date
			  ORDER BY date
