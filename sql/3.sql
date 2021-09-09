SELECT 
	dep_id,
	count(distinct emp_id) as sm
FROM employees emp
GROUP BY dep_id
HAVING count(distinct emp_id)>3