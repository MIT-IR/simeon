select
    student_id as user_id,
    count(distinct chapter_mid) as nchapters
from `{latest_dataset}.studentmodule`
join `{latest_dataset}.course_axis`
using(module_id)
where chapter_mid is not null
group by 1
order by 1