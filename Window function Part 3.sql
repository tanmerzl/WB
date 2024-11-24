CREATE TABLE query (
    searchid SERIAL PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    userid INT,
    ts BIGINT, 
    devicetype VARCHAR(50),
    deviceid VARCHAR(100),
    query TEXT
);

INSERT INTO query (year, month, day, userid, ts, devicetype, deviceid, query)
VALUES 
(2023, 10, 20, 1, 1700820000, 'android', 'device_1', 'к'),
(2023, 10, 20, 1, 1700820030, 'android', 'device_1', 'ку'),
(2023, 10, 20, 1, 1700820100, 'android', 'device_1', 'куп'),
(2023, 10, 20, 1, 1700820400, 'android', 'device_1', 'купить'),
(2023, 10, 20, 2, 1700820500, 'android', 'device_2', 'зак'),
(2023, 10, 20, 2, 1700820510, 'android', 'device_2', 'заказ'),
(2023, 10, 20, 2, 1700820530, 'android', 'device_2', 'заказ сейчас'),
(2023, 10, 20, 3, 1700820000, 'ios', 'device_3', 'зака'),
(2023, 10, 20, 3, 1700824600, 'ios', 'device_3', 'заказа'),
(2023, 10, 20, 3, 1700826600, 'ios', 'device_3', 'заказать'),
(2023, 10, 20, 4, 1700820000, 'android', 'device_4', 'купить кур'),
(2023, 10, 20, 4, 1700822010, 'android', 'device_4', 'купить куртку'),
(2023, 10, 20, 1, 1732447927, 'desktop', 'device_1_8', 'по'),
(2023, 10, 20, 1, 1732447527, 'desktop', 'device_1_8', 'поку'),
(2023, 10, 20, 1, 1732447427, 'desktop', 'device_1_8', 'покупка'),
(2023, 10, 20, 4, 1732443804, 'ios', 'device_4_5', 'заказ'),
(2023, 10, 20, 4, 1732460928, 'android', 'device_4_4', 'взя'),
(2023, 10, 20, 4, 1732460938, 'android', 'device_4_4', 'взять'),
(2023, 10, 20, 4, 1732460948, 'android', 'device_4_4', 'взять '),
(2023, 10, 20, 4, 1732460958, 'android', 'device_4_4', 'взять в доро'),
(2023, 10, 20, 4, 1732460968, 'android', 'device_4_4', 'взять в дорогу'),
(2023, 10, 20, 5, 1732447427, 'android', 'device_10', 'покупка'),
(2023, 10, 20, 5, 1732447537, 'android', 'device_10', 'зака');

ALTER TABLE query ADD COLUMN is_final INT;

WITH ranked_queries AS (
    SELECT 
        year, month, day, userid, ts, devicetype, deviceid, query,
        LEAD(query) OVER (PARTITION BY userid, deviceid ORDER BY ts) AS next_query,
        LEAD(ts) OVER (PARTITION BY userid, deviceid ORDER BY ts) AS next_ts,
        LEAD(LENGTH(query)) OVER (PARTITION BY userid, deviceid ORDER BY ts) AS next_query_length
    FROM query
),
is_final_calculated AS (
    SELECT 
        year, month, day, userid, ts, devicetype, deviceid, query, next_query,
        CASE 
            WHEN next_ts IS NULL THEN 1
            WHEN next_ts - ts > 180 THEN 1
            WHEN next_ts - ts > 60 AND LENGTH(query) > next_query_length THEN 2
            ELSE 0
        END AS is_final
    FROM ranked_queries
)
SELECT year, month, day, userid, ts, devicetype, deviceid, query, next_query, is_final
FROM is_final_calculated
WHERE year = 2023 AND month = 10 AND day = 20
  AND devicetype = 'android'
  AND is_final IN (1, 2);

