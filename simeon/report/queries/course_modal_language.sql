SELECT
    username,
    course_id,
    resource,
    resource_event_data as language,
    transcript_download as language_download,
    n_events as language_nevents,
    n_diff_lang as language_ndiff
FROM 
    (
		SELECT
		    username,
		    course_id,
		    resource,
		    resource_event_data,
		    transcript_download,
		    n_events,
		    last_time_used_lang,
		    MAX(last_time_used_lang) OVER (PARTITION BY username ) AS max_time_used_lang,
		    n_diff_lang
		FROM `{latest_dataset}.language_multi_transcripts`
		WHERE
		    rank_num = 1
		ORDER BY
		    username ASC 
    )
WHERE
	last_time_used_lang = max_time_used_lang