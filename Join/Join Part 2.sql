 -- JOIN 
 -- Часть 2. Задание 1
 -- Вычислит общую сумму продаж для каждой категории продуктов.
 SELECT 
	p.product_category, 
    SUM(o.order_ammount) AS product_total_amount
FROM 
	products_3 p
JOIN 
	orders_2 o
	USING (product_id)
GROUP BY 
	p.product_category
ORDER BY 
	product_total_amount DESC;
    
 -- Часть 2. Задание 2
-- Определит категорию продукта с наибольшей общей суммой продаж.
 WITH ProductsAmount AS(
	SELECT
  		o.product_id,
  		p.product_name,
  		p.product_category,
  		SUM(o.order_ammount) AS product_total_amount
  	FROM 
  		orders_2 o
  	JOIN products_3 p 
  		USING(product_id)
  	GROUP BY 
  		o.product_id, p.product_name, p.product_category)
SELECT 
	--pa.product_name, -- посмотреть название продукта
    --pa.product_total_amount, -- и его суммарную выручку
  	pa.product_category
    
FROM 
	ProductsAmount pa
WHERE 
	pa.product_total_amount = (SELECT MAX(product_total_amount) FROM ProductsAmount);
  		
 -- Часть 2. Задание 3
 -- Для каждой категории продуктов, определит продукт с максимальной 
 -- суммой продаж в этой категории
WITH ProductsAmount AS(
	SELECT
  		o.product_id,
  		p.product_name,
  		p.product_category,
  		SUM(o.order_ammount) AS product_total_amount
  	FROM 
  		orders_2 o
  	JOIN products_3 p 
  		USING(product_id)
  	GROUP BY 
  		o.product_id, p.product_name, p.product_category)
SELECT DISTINCT ON(pa.product_category)
	pa.product_category,
	pa.product_name
    --pa.product_total_amount -- посмотреть суммарную выручку
    
FROM 
	ProductsAmount pa
order by 
	pa.product_category, pa.product_total_amount DESC;
  		
