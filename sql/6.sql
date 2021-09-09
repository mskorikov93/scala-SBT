SELECT
	sg.grade as grade,
	count(emp.emp_id) as cnt
FROM employees as emp
JOIN salary_grade as sg 
	on emp.salary>=sg.min_salary and emp.salary<=sg.max_salary 
GROUP BY sg.grade 
ORDER BY sg.grade desc