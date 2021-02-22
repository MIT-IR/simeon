select 
    user_id, 
    count(*) as nchapters 
from (
        SELECT 
            student_id as user_id, 
            module_id, 
            count(*) as chapter_views
        FROM `{latest_dataset}.studentmodule`
        where module_type = "chapter"
        group by user_id, module_id
    )
group by user_id
order by user_id