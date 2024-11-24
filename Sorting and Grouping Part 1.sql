-- # Часть 1. Задание 1
    SELECT 
		city, 
		age, 
		COUNT(id) AS buyers
	FROM 
		users
	GROUP BY 
		city, 
		age
	ORDER BY 
		buyers DESC;

-- Для категорий
	SELECT 
	    city,
	    CASE 
	        WHEN age BETWEEN 0 AND 20 THEN 'young'
	        WHEN age BETWEEN 21 AND 49 THEN 'adult'
	        WHEN age >= 50 THEN 'old'
	        ELSE 'unknown'
	    END AS age_category,
	    COUNT(*) AS buyers
	FROM 
	    users
	GROUP BY 
	    city, 
	    age_category
	ORDER BY 
	    city, 
	    buyers DESC;
        
 -- Часть 1. Задание 2
 	SELECT 
	    category,
	    ROUND(AVG(price), 2) AS avg_price
	FROM 
	    products
	WHERE 
	    name LIKE '%hair%' OR name LIKE '%home%'
	GROUP BY category;
    
