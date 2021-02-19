select 
    user_id, 
    count(*) as nchapters 
from (
        SELECT 
            student_id as user_id, 
            module_id, 
            count(*) as chapter_views
        FROM `mitir-mitx.MITx__6_00_1x__1T2019_latest.studentmodule`
        where module_type = "chapter"
        group by user_id, module_id
    )
group by user_id
order by user_id