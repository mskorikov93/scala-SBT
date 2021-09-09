SELECT 
	emp.emp_name,
	emp.job_name,
	emp.dep_id,
	emp.salary as employeeSalary,
	man.salary as managerSalary 
FROM employees emp
JOIN employees man on emp.manager_id = man.emp_id
WHERE emp.salary>man.salary