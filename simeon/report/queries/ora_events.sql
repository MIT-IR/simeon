SELECT 
    (
        case when module_id_sometimes is not null then REGEXP_EXTRACT(module_id_sometimes, ".*/([^/]+)")
        when submission_uuid is not null then REGEXP_EXTRACT(last_module_id, ".*/([^/]+)")
        end
    ) as problem_id,
    *,
    (
        case when module_id_sometimes is not null then module_id_sometimes
        when submission_uuid is not null then last_module_id
        end
    ) as module_id,
FROM (
    SELECT 
        *,
        -- must aggregate to get module_id, which is not available for some events, like peer_assess
        -- this means module_id may be missing, e.g. if the creation event happens in a different day's tracking log
        NTH_VALUE(module_id_sometimes, 1) 
            over (partition by submission_uuid order by module_id_sometimes desc) last_module_id,
    FROM (
        SELECT
        time,
        username,
        '{{course_id}}' as course_id,
        (
            case when REGEXP_CONTAINS(event_type, "openassessmentblock..*") then 
                REGEXP_EXTRACT(event_type, "openassessmentblock.(.*)")
            when REGEXP_CONTAINS(event_type, "/courses/.*") then
                REGEXP_EXTRACT(event_type, "/courses/.*/(handler/.*)")
            end
        ) as action,
        (
            case when event_type = "openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                when event_type = "openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                when event_type = "openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.submission_uuid")
                when event_type = "openassessmentblock.get_peer_submission" then JSON_EXTRACT_SCALAR(event, "$.submission_returned_uuid")
            end
        ) as submission_uuid,
        (
            case when event_type="openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].option.points")
                when event_type="openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].option.points")
            end
        ) as part1_points,
        (
            case when event_type="openassessmentblock.peer_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].feedback")
                when event_type="openassessmentblock.self_assess" then JSON_EXTRACT_SCALAR(event, "$.parts[0].feedback")
            end
        ) as part1_feedback,
        (
            case when event_type="openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.attempt_number")
            end
        ) as attempt_num,
        (
            case when event_type="openassessmentblock.get_peer_submission" then JSON_EXTRACT_SCALAR(event, "$.item_id")
                when event_type="openassessmentblock.create_submission" then
                    REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(event, "$.answer.file_key"), ".*(i4x://.*)")
                when REGEXP_CONTAINS(event_type, "/courses/.*/xblock/i4x:.*") then
                    REGEXP_REPLACE(REGEXP_EXTRACT(event_type, "/courses/.*/xblock/(i4x:;_[^/]+)/handler.*" ),";_", "/")
            end
        ) as module_id_sometimes,
        event_type,
        event,
        event_source,
        (case when event_type="openassessmentblock.create_submission" then JSON_EXTRACT_SCALAR(event, "$.answer.text") end) as answer_text,
        FROM `{{ log_dataset }}.tracklog_*`
        WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
        event_type LIKE '%openassessment%' AND event != {% raw %} '{{"POST": {{}}, "GET": {{}}}}' {% endraw %}
        order by time
        )
    order by time
    )
order by time