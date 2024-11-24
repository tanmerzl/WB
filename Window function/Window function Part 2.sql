 -- Window functions
 
 -- Переименование столбцов таблиц 
 -- В самом конце будет запрос для обратного переименнования. 
 
 -- Таблица sales
ALTER TABLE "SALES" RENAME TO sales;
ALTER TABLE sales RENAME COLUMN "DATE" TO date;
ALTER TABLE sales RENAME COLUMN "SHOPNUMBER" TO shopnumber;
ALTER TABLE sales RENAME COLUMN "ID_GOOD" TO id_good;
ALTER TABLE sales RENAME COLUMN "QTY" TO qty;

-- Таблица goods
ALTER TABLE "GOODS" RENAME TO goods;
ALTER TABLE goods RENAME COLUMN "ID_GOOD" TO id_good;
ALTER TABLE goods RENAME COLUMN "CATEGORY" TO category;
ALTER TABLE goods RENAME COLUMN "GOOD_NAME" TO good_name;
ALTER TABLE goods RENAME COLUMN "PRICE" TO price;

-- Таблица shops
ALTER TABLE "SHOPS" RENAME TO shops;
ALTER TABLE shops RENAME COLUMN "SHOPNUMBER" TO shopnumber;
ALTER TABLE shops RENAME COLUMN "CITY" TO city;
ALTER TABLE shops RENAME COLUMN "ADDRESS" TO address;

-- # Часть 2. Задание 1

SELECT DISTINCT
    s.shopnumber,
    sh.city,
    sh.address,
    SUM(s.qty) OVER (PARTITION BY s.shopnumber) AS sum_qty,
    SUM(s.qty::INTEGER * g.price) OVER (PARTITION BY s.shopnumber) AS sum_qty_price
FROM 
    sales s
JOIN 
    goods g ON s.id_good = g.id_good
JOIN 
    shops sh ON s.shopnumber = sh.shopnumber
WHERE 
    s.date = '02/01/2016'
ORDER BY 
    s.shopnumber;

-- # Часть 2. Задание 2

SELECT 
    s.date AS date_,
    sh.city,
    --SUM(s.qty::INTEGER * g.price) AS sum_qty_price, 
    --SUM(SUM(s.qty::INTEGER * g.price)) OVER (PARTITION BY s.date) AS total_sales,
    ROUND(SUM(s.qty::INTEGER * g.price) / SUM(SUM(s.qty::INTEGER * g.price)) OVER (PARTITION BY s.date), 3) AS sum_sales_rel
FROM 
    sales s
JOIN 
    goods g ON s.id_good = g.id_good
JOIN 
    shops sh ON s.shopnumber = sh.shopnumber
WHERE 
    g.category = 'ЧИСТОТА'
GROUP BY 
    s.date, sh.city
ORDER BY 
    s.date, sh.city;

-- # Часть 2. Задание 3

SELECT    
	date,
    shopnumber,
    id_good,row_num
FROM     
    (SELECT 
        s.date,
        s.shopnumber,
        s.id_good,
        s.qty,
        ROW_NUMBER() OVER (PARTITION BY s.date, s.shopnumber ORDER BY s.qty DESC) AS row_num
    FROM 
        sales s)
WHERE 
	row_num <= 3
ORDER BY 
	date, shopnumber, row_num;

-- # Часть 2. Задание 4
-- Не выводятся значения, когда в предыдущий день не было продаж или о них неизвестно
WITH sales_with_prev AS ( 
    SELECT 
        s.date AS date_,
        s.shopnumber,
        g.category,
        LAG(SUM(s.qty::INTEGER * g.price)) OVER (
            PARTITION BY s.shopnumber, g.category 
            ORDER BY s.date
        ) AS prev_sales
    FROM 
        sales s
    JOIN 
        goods g ON s.id_good = g.id_good
    JOIN 
        shops sh ON s.shopnumber = sh.shopnumber
    WHERE 
        sh.city = 'СПб'
    GROUP BY 
        s.date, s.shopnumber, g.category)
SELECT 
    *
FROM 
    sales_with_prev
WHERE 
    prev_sales IS NOT NULL
ORDER BY 
    date_, shopnumber, category;



-- Обратное переименование

-- Таблица sales
ALTER TABLE sales RENAME COLUMN date TO "DATE";
ALTER TABLE sales RENAME COLUMN shopnumber TO "SHOPNUMBER";
ALTER TABLE sales RENAME COLUMN id_good TO "ID_GOOD";
ALTER TABLE sales RENAME COLUMN qty TO "QTY";
ALTER TABLE sales RENAME TO "SALES";

-- Таблица goods
ALTER TABLE goods RENAME COLUMN id_good TO "ID_GOOD";
ALTER TABLE goods RENAME COLUMN category TO "CATEGORY";
ALTER TABLE goods RENAME COLUMN good_name TO "GOOD_NAME";
ALTER TABLE goods RENAME COLUMN price TO "PRICE";
ALTER TABLE goods RENAME TO "GOODS";

-- Таблица shops
ALTER TABLE shops RENAME COLUMN shopnumber TO "SHOPNUMBER";
ALTER TABLE shops RENAME COLUMN city TO "CITY";
ALTER TABLE shops RENAME COLUMN address TO "ADDRESS";
ALTER TABLE shops RENAME TO "SHOPS";
