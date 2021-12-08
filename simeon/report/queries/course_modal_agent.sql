SELECT 
    username, 
    agent_type as modal_agent, 
    agent_count,
    mobile,
    desktop
FROM
    ( 
        SELECT 
            username, 
            agent_type, 
            agent_count,
            mobile,
            desktop,
            ROW_NUMBER() over (partition by username order by agent_count DESC) rank,
        from 
            ( 
                select 
                    username, 
                    agent_type, 
                    sum(agent_count) as agent_count,
                    max(mobile) as mobile,
                    max(desktop) as desktop
            from `{latest_dataset}.pc_day_agent_counts` 
            where agent_type is not null and agent_type != ""
            GROUP BY username, agent_type
    )
    )
    where rank=1
    order by username