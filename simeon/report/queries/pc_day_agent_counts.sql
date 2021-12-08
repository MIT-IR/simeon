SELECT 
    username, 
    case
        when upper(agent) like '%IPHONE%' then 'Mobile'
        when upper(agent) like '%ANDROID%' then 'Mobile'
        when upper(agent) like '%WINDOWS%' then 'Desktop'
        when upper(agent) like '%MACINTOSH%' then 'Desktop'
        when upper(agent) like '%EDX.MOBILE%' then 'Mobile'
        when upper(agent) like '%IPAD%' then 'Mobile'
        when upper(agent) like '%LINUX%' or upper(agent) like '%BSD%' then 'Desktop'
        when upper(agent) like '%CROS%' then 'Desktop'
        when upper(agent) like '%DARWIN%' then 'Mobile'
    else 'Unknown'
    end as agent_type,
    case 
        when upper(agent) like '%IPHONE%' then 1
        when upper(agent) like '%ANDROID%' then 1
        when upper(agent) like '%EDX.MOBILE%' then 1
        when upper(agent) like '%IPAD%' then 1
        when upper(agent) like '%DARWIN%' then 1
    else 0
    end as mobile,
    case
        when upper(agent) like '%WINDOWS%' then 1
        when upper(agent) like '%MACINTOSH%' then 1
        when upper(agent) like '%LINUX%' or upper(agent) like '%BSD%' then 1
        when upper(agent) like '%CROS%' then 1
    else 0
    end as desktop,
    date(time) as date, 
    count(*) as agent_count,
    '{course_id}' as course_id,
FROM `{log_dataset}.tracklog_*`
where username != '' and agent != ''
group by username, agent_type, mobile, desktop, date
order by date 