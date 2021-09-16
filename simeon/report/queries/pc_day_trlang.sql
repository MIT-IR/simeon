SELECT
		  username,
		  course_id,
		  DATE(time) AS date,
		  MAX(time) AS last_time,
		  resource,
		  CASE
		    WHEN resource_event_type = 'transcript_download' AND prev_event_data IS NOT NULL THEN prev_event_data
		    WHEN resource_event_type = 'transcript_download'
		        AND prev_event_data IS NULL THEN 'en'
		    WHEN resource_event_type = 'transcript_language' THEN resource_event_data
		    ELSE resource_event_data
		  END AS resource_event_data,
		  resource_event_type,
		  SUM(lang_count) AS langcount
		FROM (
		  SELECT
		    course_id,
		    username,
		    resource,
		    resource_event_data,
		    resource_event_type,
		    LAG(time, 1) OVER (PARTITION BY username ORDER BY time) AS prev_time,
		    LAG(resource_event_type, 1) OVER (PARTITION BY username ORDER BY time) AS prev_event_type,
		    LAG(resource_event_data, 1) OVER (PARTITION BY username ORDER BY time) AS prev_event_data,
		    time,
		    COUNT(*) AS lang_count,
		    event_type
		  FROM 
            (
                SELECT
                    time,
                    course_id,
                    username,
                    'video' AS resource,
                    CASE
                    WHEN module_id IS NOT NULL THEN REGEXP_EXTRACT(module_id, r'.*video/(.*)')                         
                    ELSE REGEXP_EXTRACT(event_type, r'.*;_video;_(.*)/handler/transcript/translation/.*') END   -- Older data
                    AS resource_id,
                    CASE
                    WHEN (event_type = 'edx.ui.lms.link_clicked'                                                    
                    AND REGEXP_CONTAINS(JSON_EXTRACT(event, '$.target_url'), r'(.*handler/transcript/download)') ) THEN 'transcript_download'
                    WHEN (REGEXP_CONTAINS(event_type, "/transcript/translation/.*") ) THEN 'transcript_language'
                    ELSE NULL
                    END AS resource_event_type,
                    REGEXP_EXTRACT(event_type, r'.*/handler/transcript/translation/(.*)') AS resource_event_data,
                    event_type
                FROM `{{ log_dataset }}.tracklog_*`
				WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
                time > TIMESTAMP("2010-10-01 01:02:03")
                AND username != ""
                AND (
						(event_type NOT LIKE "%/xblock/%" AND event_type = 'edx.ui.lms.link_clicked' AND REGEXP_CONTAINS(JSON_EXTRACT(event, '$.target_url'), r'(.*handler/transcript/download)')) 
                    	OR (REGEXP_CONTAINS(event_type, "/transcript/translation/.*"))
					)
		    	ORDER BY time
			)
		  GROUP BY
		    username,
		    course_id,
		    resource,
		    resource_event_data,
		    resource_event_type,
		    event_type,
		    time )
		GROUP BY
		  username,
		  course_id,
		  date,
		  resource,
		  resource_event_data,
		  resource_event_type
		ORDER BY
		  date ASC,
		  username ASC