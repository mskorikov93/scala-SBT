SELECT 
	Emp_id,
	Job_name,
	Dep_id,
	DATEDIFF(day,hire_date,current_date) as experience,
	row_number() over(partition by  dep_id order by Datediff(day,hire_date,current_date) desc) as [range]
FROM employees