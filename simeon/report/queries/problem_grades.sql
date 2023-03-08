SELECT 
    student_id as user_id,
    module_id,
    SAFE_CAST(grade as FLOAT64) as grade,
    SAFE_CAST(max_grade as FLOAT64) as max_grade,
    case 
        when SAFE_CAST(grade as FLOAT64) >= SAFE_CAST(max_grade as FLOAT64) then true 
        else false 
    end as perfect,
    case 
        when grade is null or grade = "NULL" or grade = "" then false 
        else true 
    end as attempted,
FROM `{latest_dataset}.studentmodule`
WHERE module_type = "problem"
