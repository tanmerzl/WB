 -- Часть 2. Задание 1
 -- Здесь и далее считаем, что нужны именно уникальные категории товаров у каждого продавца. 
 SELECT 
	seller_id,
    Count(DISTINCT category) as total_categ,
    ROUND(AVG(rating), 1) AS avg_rating,
    SUM(revenue) AS total_revenue,
    CASE 
    	WHEN Count(DISTINCT category) > 1 AND SUM(revenue) > 50000 THEN 'rich'
    	WHEN Count(DISTINCT category) > 1 AND SUM(revenue) <= 50000 THEN 'poor'
        ELSE NULL
    END AS seller_type
FROM
	sellers
WHERE 
	category != 'Bedding'
Group BY 
	seller_id
HAVING Count(DISTINCT category) > 1
ORDER by seller_id;


-- Преобразуем формат дат(date_reg и date) в 'YYYY-MM-DD'
-- Если не соответствует формату, оставляем как есть
-- В date_reg иногда пропущены нули в днях и месяцах, например '7/4/2015' и '27/9/2022'
-- Эти случаи рассматриваются отдельно
UPDATE sellers
SET 
    date_reg = CASE 
        WHEN LENGTH(date_reg) = 8 THEN
            -- Формат '7/4/2015'
            SUBSTR(date_reg, 5, 4) || '-0' || SUBSTR(date_reg, 3, 1) || '-0' || SUBSTR(date_reg, 1, 1)
        WHEN LENGTH(date_reg) = 9 AND SUBSTR(date_reg, 2, 1) != '/' THEN
            -- Формат '27/9/2022'
            SUBSTR(date_reg, 6, 4) || '-0' || SUBSTR(date_reg, 4, 1) || '-' || SUBSTR(date_reg, 1, 2)
        WHEN LENGTH(date_reg) = 9 AND SUBSTR(date_reg, 2, 1) = '/' THEN
            -- Формат '7/12/2016'
            SUBSTR(date_reg, 6, 4) || '-' || SUBSTR(date_reg, 3, 2) || '-0' || SUBSTR(date_reg, 1, 1)
        WHEN LENGTH(date_reg) = 10 THEN
            -- Формат '17/12/2016'
            SUBSTR(date_reg, 7, 4) || '-' || SUBSTR(date_reg, 4, 2) || '-' || SUBSTR(date_reg, 1, 2)
        ELSE date_reg 
    END,
    date = CASE 
        WHEN LENGTH(date) = 10 THEN
            -- Преобразование 'DD/MM/YYYY' в 'YYYY-MM-DD'
            SUBSTR(date, 7, 4) || '-' || SUBSTR(date, 4, 2) || '-' || SUBSTR(date, 1, 2)
        ELSE date 
    END;
    
 -- Часть 2. Задание 2
 
 WITH filtered_sellers AS (
    SELECT seller_id
    FROM sellers
    WHERE category != 'Bedding'
    GROUP BY seller_id
    HAVING 
        COUNT(DISTINCT category) > 1
        AND SUM(revenue) <= 50000
),

max_min_delivery AS (
    SELECT 
        MAX(delivery_days) - MIN(delivery_days) AS max_delivery_difference
    FROM sellers
    WHERE seller_id IN (SELECT seller_id FROM filtered_sellers)
)
SELECT DISTINCT ON (seller_id)
    seller_id,
    FLOOR((CURRENT_DATE - TO_DATE(date_reg, 'YYYY-MM-DD')) / 30) AS month_from_registration,
    (SELECT max_delivery_difference FROM max_min_delivery) AS max_delivery_difference
FROM sellers
WHERE seller_id IN (SELECT seller_id FROM filtered_sellers)
ORDER BY seller_id, month_from_registration;

 -- Часть 2. Задание 3
/* Данное решение предполагает, что самая первая регистрация 
продавца по одной из категорий произошла в 2022 году. 
Учитываются категории, которые были зарегистрированы 
не только в 2022 году, но и в любые другие года.*/

WITH first_registration AS (
    SELECT 
        seller_id,
        MIN(date_reg::date) AS first_registration_date
    FROM sellers
    GROUP BY seller_id
),
filtered_sellers AS (
    SELECT 
  		seller_id
    FROM first_registration 
    WHERE EXTRACT(YEAR FROM first_registration_date) = 2022
),
seller_categories AS (
    SELECT 
        seller_id,
        ARRAY_AGG(DISTINCT category ORDER BY category) AS categories,
        SUM(revenue) AS total_revenue
    FROM sellers
    WHERE seller_id IN (SELECT seller_id FROM filtered_sellers)
    GROUP BY seller_id
    HAVING 
        COUNT(DISTINCT category) = 2
        AND SUM(revenue) > 75000 
)
SELECT 
    seller_id,
    ARRAY_TO_STRING(categories, ' - ') AS category_pair
FROM seller_categories
ORDER BY seller_id;

-- Данный запрос не выводит предложенных продавцов с регистрацией в 2022 году, 
--но можно проверить запрос на 2016 и 2020 годах.