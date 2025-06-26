BEGIN TRANSACTION;
MERGE INTO `{project_id}.{bigqury_dataset}.movies` t
USING (
  SELECT
    COALESCE(sm.movie_id, m.movie_id) AS movie_id,
    COALESCE(sm.imdb_id, m.imdb_id) AS imdb_id,
    COALESCE(sm.original_title, m.original_title) AS original_title,
    COALESCE(sm.title, m.title) AS title,
    COALESCE(sm.genres, m.genres) AS genres,
    COALESCE(sm.origin_country, m.origin_country) AS origin_country,
    CASE
      WHEN sm.popularity IS NULL AND m.popularity IS NOT NULL THEN ARRAY_CONCAT(m.popularity, [STRUCT(SAFE_CAST(NULL AS FLOAT64) AS value)])
      WHEN sm.popularity IS NOT NULL AND m.popularity IS NULL THEN sm.popularity
      ELSE ARRAY_CONCAT(m.popularity, sm.popularity)
    END AS popularity,
    COALESCE(sm.production_companies, m.production_companies) AS production_companies,
    COALESCE(sm.release_date, m.release_date) AS release_date,
    COALESCE(sm.runtime, m.runtime) AS runtime,
    CASE
      WHEN sm.vote_average IS NULL AND m.vote_average IS NOT NULL THEN ARRAY_CONCAT(m.vote_average, [STRUCT(SAFE_CAST(NULL AS FLOAT64) AS value)])
      WHEN sm.vote_average IS NOT NULL AND m.vote_average IS NULL THEN sm.vote_average
      ELSE ARRAY_CONCAT(m.vote_average, sm.vote_average)
    END AS vote_average,
    CASE
      WHEN sm.vote_count IS NULL AND m.vote_count IS NOT NULL THEN ARRAY_CONCAT(m.vote_count, [STRUCT(NULL AS value)])
      WHEN sm.vote_count IS NOT NULL AND m.vote_count IS NULL THEN sm.vote_count
      ELSE ARRAY_CONCAT(m.vote_count, sm.vote_count)
    END AS vote_count,
    COALESCE(sm.casts, m.casts) AS casts,
    COALESCE(sm.crews, m.crews) AS crews,
    COALESCE(sm.opening_gross, m.opening_gross) AS opening_gross,
    COALESCE(sm.movie_budget, m.movie_budget) AS movie_budget,
    CASE
      WHEN sm.gross IS NULL AND m.gross IS NOT NULL THEN ARRAY_CONCAT(m.gross, [STRUCT(NULL AS value)])
      WHEN sm.gross IS NOT NULL AND m.gross IS NULL THEN sm.gross
      ELSE ARRAY_CONCAT(m.gross, sm.gross)
    END AS gross,
    CASE
      WHEN sm.current_date IS NULL AND m.current_date IS NOT NULL THEN ARRAY_CONCAT(m.current_date, [STRUCT(SAFE_CAST('{date}' AS DATE) AS value)])
      WHEN sm.current_date IS NOT NULL AND m.current_date IS NULL THEN sm.current_date
      ELSE ARRAY_CONCAT(m.current_date, sm.current_date)
    END AS current_date
  FROM `{project_id}.{bigqury_dataset}.movies` AS m
  FULL JOIN `{project_id}.{bigqury_dataset}.staging_movies` AS sm
  ON m.movie_id = sm.movie_id
) u
ON t.movie_id = u.movie_id
WHEN MATCHED THEN
UPDATE SET
  t.popularity = u.popularity,
  t.vote_average = u.vote_average,
  t.vote_count = u.vote_count,
  t.opening_gross = u.opening_gross,
  t.movie_budget = u.movie_budget,
  t.gross = u.gross,
  t.current_date = u.current_date
WHEN NOT MATCHED THEN
    INSERT (
        movie_id,
        imdb_id,
        original_title,
        title,
        genres,
        origin_country,
        popularity,
        production_companies,
        release_date,
        runtime,
        vote_average,
        vote_count,
        casts,
        crews,
        opening_gross,
        movie_budget,
        gross,
        current_date
    )
    VALUES (
        u.movie_id,
        u.imdb_id,
        u.original_title,
        u.title,
        u.genres,
        u.origin_country,
        u.popularity,
        u.production_companies,
        u.release_date,
        u.runtime,
        u.vote_average,
        u.vote_count,
        u.casts,
        u.crews,
        u.opening_gross,
        u.movie_budget,
        u.gross,
        u.current_date
    );

MERGE `{project_id}.{bigqury_dataset}.credits` c
USING `{project_id}.{bigqury_dataset}.staging_credits` s
ON s.person_id = c.person_id
WHEN MATCHED THEN
UPDATE SET
  c.name = s.name,
  c.gender = s.gender,
  c.department = s.department
WHEN NOT MATCHED THEN
  INSERT (
    person_id,
    name,
    gender,
    department
  )
  VALUES (
    s.person_id,
    s.name,
    s.gender,
    s.department
  );

MERGE `{project_id}.{bigqury_dataset}.production_companies` p
USING `{project_id}.{bigqury_dataset}.staging_production_companies` sp
ON sp.company_id = p.company_id
WHEN MATCHED THEN
UPDATE SET
  p.company_name = sp.company_name,
  p.company_country = sp.company_country
WHEN NOT MATCHED THEN
  INSERT (
    company_id,
    company_name,
    company_country
  )
  VALUES (
    sp.company_id,
    sp.company_name,
    sp.company_country
  );
COMMIT TRANSACTION;