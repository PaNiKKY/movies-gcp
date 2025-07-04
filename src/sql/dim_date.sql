CREATE OR REPLACE TABLE
  accident_data.dim_date AS
SELECT
  CAST(REPLACE(CAST(date AS STRING), '-','') AS NUMERIC) AS date_key,
  date AS date_id,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(QUARTER FROM date) AS quarter,
  FORMAT_DATE('%Q', date) AS quarter_name,
  EXTRACT(MONTH FROM date) AS month,
  FORMAT_DATE('%B', date) AS month_name,
  FORMAT_DATE('%b', date) AS month_name_short,
  EXTRACT(DAY FROM date) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  FORMAT_DATE('%A', date) AS day_name,
  FORMAT_DATE('%a', date) AS day_name_short,
  EXTRACT(WEEK FROM date) AS week_of_year,
  EXTRACT(DAYOFYEAR FROM date) AS day_of_year,
  CASE
    WHEN FORMAT_DATE('%A', date) IN ('Saturday', 'Sunday') THEN TRUE
  ELSE
    FALSE
  END AS is_weekend,
FROM
  (
    SELECT *
    FROM UNNEST(GENERATE_DATE_ARRAY('2008-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
  )
GROUP BY
  date
ORDER BY
  date_key;