SELECT 
    student_id as user_id,
    module_id,
    case 
        when grade is not null and grade !="NULL" then CAST(grade as FLOAT64) 
    end as grade,
    CAST(max_grade as FLOAT64) as max_grade,
    case 
        when CAST(grade as FLOAT64) >= CAST(max_grade as FLOAT64) then true 
        else false 
    end as perfect,
    case 
        when grade is null or grade = "NULL" then false 
        else true 
    end as attempted,
FROM `{latest_dataset}.studentmodule`
WHERE module_type = "problem"