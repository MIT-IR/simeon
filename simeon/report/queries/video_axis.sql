-- Table that keeps track of information relating to the video lectures in the course
SELECT chapters.index as index_chapter,
videos.index as index_video,
videos.category as category,
videos.course_id as course_id,
videos.name as name,
videos.vid_id as video_id,
videos.yt_id as youtube_id,
chapters.name as chapter_name,
case when videos.video_length is null then videos.bundle_duration else videos.video_length end as video_length
FROM (
    SELECT index, category, course_id, name, chapter_mid,
    REGEXP_EXTRACT(REGEXP_REPLACE(module_id, '[.]', '_'), r'(?:.*\/)(.*)') as vid_id,
    ARRAY_REVERSE(SPLIT(data.ytid, ':'))[SAFE_ORDINAL(1)] as yt_id,
    data.duration as bundle_duration,
    {% if youtube_table is defined and youtube_table %}
    youtubes.duration as video_length
    {% else %}
    data.duration as video_length
    {% endif %}
    FROM `{latest_dataset}.course_axis`
    {% if youtube_table is defined and youtube_table %}
    LEFT JOIN
    `{youtube_table}` youtubes
    ON youtubes.id = ARRAY_REVERSE(SPLIT(data.ytid, ':'))[SAFE_ORDINAL(1)]
    {% endif %}
    WHERE category = "video"
    AND (NOT visible_to_staff_only OR visible_to_staff_only IS NULL)
) as videos
LEFT JOIN (
    SELECT name, module_id, index
    FROM `{latest_dataset}.course_axis`
) as chapters
ON videos.chapter_mid = chapters.module_id
ORDER BY videos.index asc