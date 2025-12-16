
-- Total number of rows;
SELECT COUNT(*) AS total_rows FROM sensor_readings;

-- Number of distinct sensors present on the database;
SELECT COUNT(DISTINCT name) AS distinct_sensors FROM sensor_readings;


-- Number of rows for the sensor PPL340;
SELECT COUNT(*) AS rows_for_PPL340 
FROM sensor_readings
WHERE name = 'PPL340';

-- The number of rows by year for the sensor PPL340;
SELECT year, COUNT(*) AS rows_by_year
FROM sensor_readings
WHERE name = 'PPL340'
GROUP BY year;


-- Average number of readings by year for the sensor PPL340;
SELECT AVG(yearly_count) AS avg_readings_per_year
FROM (
    SELECT year, COUNT(*) AS yearly_count
    FROM sensor_readings
    WHERE name = 'PPL340'
    GROUP BY year
) AS yearly_counts;


-- For PPL340, Identify the years in which the number of readings is less than the average;
WITH avg_readings AS (
    SELECT AVG(yearly_count) AS avg
    FROM (
        SELECT year, COUNT(*) AS yearly_count
        FROM sensor_readings
        WHERE name = 'PPL340'
        GROUP BY year
    ) AS yearly_counts
)
SELECT year
FROM (
    SELECT year, COUNT(*) AS yearly_count
    FROM sensor_readings
    WHERE name = 'PPL340'
    GROUP BY year
) AS yearly_counts
WHERE yearly_count < (SELECT avg FROM avg_readings)
ORDER BY year;



