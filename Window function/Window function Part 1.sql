 -- Window functions
 -- Часть 1. Задание 1
 
-- Предполагается, что у сотрудников в отделе не может быть двух одинаковых максимальных зарплат.
-- Первый способ решения
SELECT 
	first_name,
    last_name,
    salary,
    industry,
    FIRST_VALUE(first_name) OVER w AS name_highest_sal
FROM salary
WINDOW w AS 
	(PARTITION BY industry
     ORDER BY salary DESC);
     
-- Второй способ решения
SELECT 
    s.first_name,
    s.last_name,
    s.salary,
    s.industry,
    (SELECT first_name 
     FROM salary AS sal 
     WHERE sal.industry = s.industry 
     AND sal.salary = (
         SELECT Max(salary) 
         FROM salary 
         WHERE industry = s.industry)
     LIMIT 1
    ) AS name_highest_sal
FROM salary s
ORDER BY industry;

 -- Часть 1. Задание 2
 -- Первый способ решения
 SELECT 
	first_name,
    last_name,
    salary,
    industry,
    FIRST_VALUE(first_name) OVER w AS name_lowest_sal
FROM salary
WINDOW w AS 
	(PARTITION BY industry
     ORDER BY salary ASC);
     
-- Второй способ решения
SELECT 
    s.first_name,
    s.last_name,
    s.salary,
    s.industry,
    (SELECT first_name 
     FROM salary AS sal 
     WHERE sal.industry = s.industry 
     AND sal.salary = (
         SELECT MIN(salary) 
         FROM salary 
         WHERE industry = s.industry)
     LIMIT 1
    ) AS name_lowest_sal
FROM salary s
ORDER BY industry;  		
