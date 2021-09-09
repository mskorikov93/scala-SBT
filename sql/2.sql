SELECT 
	em.emp_name,
	em.job_name,
	em.dep_id,
	em.salary 
FROM
(SELECT 
	emp_name,
	job_name,
	dep_id,
	salary,
	rank() over(partition by dep_id order by salary asc) rn
FROM employees emp) em
WHERE em.rn = 1