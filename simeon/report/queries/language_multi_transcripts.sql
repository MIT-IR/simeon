SELECT
	username,
	course_id,
	resource,
	resource_event_data,
	SUM(transcript_download) as transcript_download,
	SUM(n_events) AS n_events,
	MAX(last_time_used_lang) AS last_time_used_lang,
	COUNT(resource_event_data) OVER (PARTITION BY username ) AS n_diff_lang,
	RANK() OVER (PARTITION BY username ORDER BY sum(n_events) DESC) AS rank_num,
FROM (
	SELECT
		username,
		course_id,
		resource,
		resource_event_data,
		resource_event_type,
		SUM(CASE
		WHEN resource_event_type = 'transcript_download' THEN 1
		ELSE 0 END) AS transcript_download,
		SUM(langcount) AS n_events,
		MAX(last_time) AS last_time_used_lang
	FROM
		`{latest_dataset}.pc_day_trlang`
	GROUP BY
		username,
		course_id,
		resource,
		resource_event_data,
		resource_event_type
	ORDER BY
		username ASC
)
GROUP BY
	username,
	course_id,
	resource,
	resource_event_data
ORDER BY
	username,
	rank_num ASC