CREATE TABLE movies_dataset.movies_temp AS (
  SELECT
    movie_id,
    imdb_id,
    original_title,
    title,
    genres,
    origin_country,
    production_companies,
    release_date,
    runtime,
    casts,
    crews,
    opening_gross,
    movie_budget,
    ARRAY(
      SELECT AS STRUCT 
        popularity.value AS popularity,
        vote_count.value AS vote_count,
        vote_average.value AS vote_average,
        gross.value AS gross,
        collect_date.value AS collect_date
      FROM UNNEST(popularity) popularity WITH OFFSET AS pop_off
      JOIN UNNEST(gross) gross WITH OFFSET AS gross_off
      JOIN UNNEST(m.current_date) collect_date WITH OFFSET AS col_off
      JOIN UNNEST(m.vote_count) vote_count WITH OFFSET AS vote_count_off
      JOIN UNNEST(m.vote_average) vote_average WITH OFFSET AS vote_average_off
      WHERE pop_off = gross_off
      AND pop_off = col_off
      AND pop_off = vote_count_off
      AND pop_off = vote_average_off
    ) AS metrics
  FROM `movies_dataset.movies` m 
);

DROP TABLE movies_dataset.movies;

ALTER TABLE movies_dataset.movies_temp
RENAME TO movies;