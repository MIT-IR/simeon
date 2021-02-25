SELECT 
    username, 
    IP as modal_ip, 
    ip_count, 
    n_different_ip
FROM
    ( 
        SELECT 
            username, 
            ip, 
            ip_count,
            ROW_NUMBER() over (partition by username order by ip_count ASC) n_different_ip,
            ROW_NUMBER() over (partition by username order by ip_count DESC) rank,
        from 
            ( 
                select 
                    username, 
                    ip, 
                    sum(ipcount) as ip_count
            from `{latest_dataset}.pc_day_ip_counts` 
            where ip is not null and ip != ""
            GROUP BY username, ip
    )
    )
    where rank=1
    order by username