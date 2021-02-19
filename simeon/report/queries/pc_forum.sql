SELECT 
    author_id as user_id, 
    count(*) as nforum,
    sum(votes.count) as nvotes,
    sum(case when pinned then 1 else 0 end) as npinned,
    sum(case when endorsed then 1 else 0 end) as nendorsed,
    sum(case when _type="CommentThread" then 1 else 0 end) as nthread,
    sum(case when _type="Comment" then 1 else 0 end) as ncomment
FROM `{latest_dataset}.forum` 
group by user_id
order by nthread desc