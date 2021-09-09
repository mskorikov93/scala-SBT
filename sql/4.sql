SELECT 
	emp.emp_name,
	emp.job_name,
	dep.dep_name
FROM employees emp
JOIN employees man on emp.manager_id = man.emp_id
LEFT JOIN department dep on dep.dep_id = emp.dep_id
WHERE emp.dep_id<>man.dep_id