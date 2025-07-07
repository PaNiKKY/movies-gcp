SELECT
  title,
  AVG(metrics.popularity) AS popularity,
  MAX(metrics.gross) AS gross
FROM `movies_dataset.movies` m,
UNNEST(metrics) AS metrics
GROUP BY 1;

WITH agg_metrics AS (
  SELECT 
    title,
    genres,
    movie_budget,
    AVG(metrics.popularity) AS popularity,
    MAX(metrics.gross) AS gross
  FROM `movies_dataset.movies` m,
  UNNEST(metrics) AS metrics
  GROUP BY 1, 2, 3
)
SELECT
  genre,
  AVG(movie_budget) AS budget,
  AVG(popularity) AS popularity,
  AVG(gross) AS gross
FROM agg_metrics,
UNNEST(genres) AS genre
WHERE movie_budget IS NOT NULL
GROUP BY 1;

WITH agg_metrics AS (
  SELECT 
    title,
    casts,
    movie_budget,
    AVG(metrics.popularity) AS popularity,
    AVG(metrics.vote_average) AS vote_average,
    AVG(metrics.vote_count) AS vote_count,
    MAX(metrics.gross) AS gross,
  FROM `movies_dataset.movies` m,
  UNNEST(metrics) AS metrics
  GROUP BY 1, 2, 3
), unnest_casts AS (
  SELECT 
    casts.person_id AS cast_id,
    popularity,
    vote_average,
    vote_count,
    gross
  FROM agg_metrics,
  UNNEST(casts) AS casts
  WHERE casts.order <= 4
)
SELECT 
  cast_id,
  c.name,
  AVG(popularity) AS popularity,
  AVG(vote_average) AS vote_average,
  AVG(vote_count) AS vote_count,
  AVG(gross) AS gross
FROM unnest_casts u
JOIN `movies_dataset.credits` c
ON c.person_id = u.cast_id
WHERE u.gross IS NOT NULL
GROUP BY 1,2