 -- JOIN 
 -- Часть 1. Задание 1
/* Клиентов с максимальным временем ожидания оказалось больше одного, 
поэтому сначала найдем максимальное время ожидания `MaxDaysWaited`, 
после сравним время ожидания клентов с этим значением. 
Считались дни, так как в данных только они имеют значение, 
если рассматривать разницу между событиями.*/
WITH MaxDaysWaited AS (
    SELECT 
        DATE_PART('day', shipment_date::timestamp - order_date::timestamp) AS max_days_waited
    FROM 
        orders
    ORDER BY 
        max_days_waited DESC
    LIMIT 1
)
SELECT 
    c.name AS customer_name,
    o.order_id,
    o.order_date,
    o.shipment_date,
    DATE_PART('day', o.shipment_date::timestamp - o.order_date::timestamp) AS days_waited
FROM 
    orders o
JOIN 
    customers c
ON 
    o.customer_id = c.customer_id
WHERE 
    DATE_PART('day', o.shipment_date::timestamp - o.order_date::timestamp) = (SELECT max_days_waited FROM MaxDaysWaited)
ORDER BY 
    o.order_id;
    
-- Часть 1. Задание 2
 SELECT 
    c.name AS customer_name,
    COUNT(o.order_id) AS total_orders,
    CAST(AVG(DATE_PART('day', o.shipment_date::timestamp - o.order_date::timestamp)) AS NUMERIC(10, 1)) AS avg_delivery_time_days,
    SUM(o.order_ammount) AS total_order_amount 
FROM 
    orders o
JOIN 
    customers c
ON 
    o.customer_id = c.customer_id
GROUP BY 
    c.name
ORDER BY 
    total_order_amount DESC;
    
-- Часть 1. Задание 3

SELECT 
    c.name AS customer_name,
    COUNT(CASE WHEN DATE_PART('day', o.shipment_date::timestamp- o.order_date::timestamp) > 5 THEN 1 END) AS delayed_orders,
    COUNT(CASE WHEN o.order_status = 'Cancel' THEN 1 END) AS canceled_orders,
    SUM(o.order_ammount) AS total_order_amount
FROM 
    orders o
JOIN 
    customers c
ON 
    o.customer_id = c.customer_id
GROUP BY 
    c.name
HAVING 
    COUNT(CASE WHEN DATE_PART('day', o.shipment_date::timestamp - o.order_date::timestamp) > 5 THEN 1 END) > 0
    OR COUNT(CASE WHEN o.order_status = 'Cancel' THEN 1 END) > 0
ORDER BY 
    total_order_amount DESC;