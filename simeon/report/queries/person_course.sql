-- Canonical dataset of learner details and activities per course
select
    distinct '{course_id}' as course_id,
    uic.user_id,
    uic.username,
    True as registered,
    IF(pc_nchapters.nchapters is null, False, True) as viewed,
    IF(
        safe_divide(
            pc_nchapters.nchapters,
            (
                SELECT COUNT(*)
                FROM `{latest_dataset}.course_axis`
                WHERE category = "chapter" AND (NOT visible_to_staff_only OR visible_to_staff_only IS NULL)
            )
        ) >= 0.5,
        True,
        False
    ) as explored,
    if(uic.certificate_status = "downloadable", true, false) as certified,
    if(grades.percent_grade >= (SELECT MAX(overall_lower_cutoff) from `{latest_dataset}.grading_policy`), True, False) as completed,
    -- if(uic.enrollment_mode = "verified", true, false) as verified,
    modal_ip.modal_ip as ip,
    {% if geo_table is defined and geo_table %}
    modal_ip.cc_by_ip,
    modal_ip.countryLabel,
    modal_ip.continent,
    modal_ip.city,
    modal_ip.region,
    modal_ip.subdivision,
    modal_ip.postalCode,
    modal_ip.un_major_region,
    modal_ip.un_economic_group,
    modal_ip.un_developing_nation,
    modal_ip.un_special_region,
    modal_ip.latitude,
    modal_ip.longitude,
    {% else %}
    null cc_by_ip,
    null countryLabel,
    null continent,
    null city,
    null region,
    null subdivision,
    null postalCode,
    null un_major_region,
    null un_economic_group,
    null un_developing_nation,
    null un_special_region,
    null latitude,
    null longitude,
    {% endif %}
    modal_agent.modal_agent,
    modal_agent.mobile as ever_mobile,
    modal_agent.desktop as ever_desktop,
    uic.profile_level_of_education as LoE,
    uic.profile_year_of_birth as YoB,
    uic.profile_gender as gender,
    grades.percent_grade as grade,
    (SELECT MAX(overall_lower_cutoff) from `{latest_dataset}.grading_policy`) as passing_grade,
    uic.enrollment_created as start_time,
    pc_day.first_event,
    pc_day.last_event,
    pc_day.nevents,
    pc_day.ndays_act,
    pc_day.nvideo as nplay_video,
    pc_nchapters.nchapters,
    pc_forum.nforum as nforum_posts,
    pc_forum.nvotes as nforum_votes,
    pc_forum.nendorsed as nforum_endorsed,
    pc_forum.nthread as nforum_threads,
    pc_forum.ncomment as nforum_comments,
    pc_forum.npinned as nforum_pinned,
    roles.roles,
    pc_day.nprogcheck,
    pc_day.nproblem_check,
    pc_forum.nforum as nforum_events,
    trim(uic.enrollment_mode) as mode,
    uic.enrollment_is_active as is_active,
    uic.certificate_created_date as cert_created_date,
    uic.certificate_modified_date as cert_modified_date,
    uic.certificate_status as cert_status,
    enroll_verified.verified_enroll_time,
    enroll_verified.verified_unenroll_time,
    uic.profile_country,
    uic.y1_anomalous,
    array_reverse(split(uic.email, "@"))[ORDINAL(1)] as email_domain,
    lang.language,
    lang.language_download,
    lang.language_nevents,
    lang.language_ndiff,
    pc_day.ntranscript,
    pc_day.nshow_answer,
    pc_day.nvideo,
    video.n_unique_videos_watched as nvideos_unique_viewed,
    video.fract_total_videos_watched as nvideos_total_watched,
    pc_day.nseq_goto,
    pc_day.nseek_video,
    pc_day.npause_video,
    pc_day.avg_dt,
    pc_day.sdv_dt,
    pc_day.max_dt,
    pc_day.n_dt,
    pc_day.sum_dt,
    roles.roles_isBetaTester,
    roles.roles_isInstructor,
    roles.roles_isStaff,
    roles.roles_isCCX,
    roles.roles_isFinance,
    roles.roles_isLibrary,
    roles.roles_isSales,
    roles.forumRoles_isAdmin,
    roles.forumRoles_isCommunityTA,
    roles.forumRoles_isModerator,
    roles.forumRoles_isStudent
from `{latest_dataset}.user_info_combo` uic
{% if geo_table is defined and geo_table %}
left join (
    select m_ip.*, geo.* except (ip)
    from `{latest_dataset}.course_modal_ip` m_ip
    left join `{geo_table}` geo
    on m_ip.modal_ip = geo.ip

) modal_ip using(username)
{% else %}
left join `{latest_dataset}.course_modal_ip` modal_ip
{% endif %}
left join `{latest_dataset}.grades_persistent` grades using(user_id)
left join `{latest_dataset}.pc_day_totals` pc_day using(username)
left join `{latest_dataset}.pc_forum` pc_forum using(user_id)
left join `{latest_dataset}.pc_nchapters` pc_nchapters using(user_id)
left join `{latest_dataset}.roles` roles using(user_id)
left join `{latest_dataset}.course_modal_language` lang using(username)
left join `{latest_dataset}.pc_video_watched` video using(user_id)
left join `{latest_dataset}.person_enrollment_verified` enroll_verified using(user_id)
left join `{latest_dataset}.course_modal_agent` modal_agent using(username)