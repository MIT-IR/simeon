SELECT chapters.index as index_chapter,
videos.index as index_video,
videos.category as category,
videos.course_id as course_id,
videos.name as name,
videos.vid_id as video_id,
videos.yt_id as youtube_id,
chapters.name as chapter_name
FROM (
    SELECT index, category, course_id, name, chapter_mid,
    REGEXP_EXTRACT(REGEXP_REPLACE(module_id, '[.]', '_'), r'(?:.*\/)(.*)') as vid_id,
    REGEXP_EXTRACT(data.ytid, r'\:(.*)') as yt_id,
    FROM `{latest_dataset}.course_axis`
    WHERE category = "video"
) as videos
LEFT JOIN (
    SELECT name, module_id, index
    FROM `{latest_dataset}.course_axis`
) as chapters
ON videos.chapter_mid = chapters.module_id
ORDER BY videos.index asc