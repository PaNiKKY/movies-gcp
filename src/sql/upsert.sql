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
    COALESCE(sm.production_companies, m.production_companies) AS production_companies,
    COALESCE(sm.release_date, m.release_date) AS release_date,
    COALESCE(sm.runtime, m.runtime) AS runtime,
    COALESCE(sm.casts, m.casts) AS casts,
    COALESCE(sm.crews, m.crews) AS crews,
    COALESCE(sm.opening_gross, m.opening_gross) AS opening_gross,
    COALESCE(sm.movie_budget, m.movie_budget) AS movie_budget,
    CASE
      WHEN sm.metrics IS NULL AND m.metrics IS NOT NULL 
        THEN ARRAY_CONCAT(
            m.metrics,
            [STRUCT(
                SAFE_CAST(NULL AS FLOAT64) AS popularity,
                SAFE_CAST(NULL AS INT64) AS vote_count,
                SAFE_CAST(NULL AS FLOAT64) AS vote_average,
                SAFE_CAST(NULL AS INT64) AS gross,
                SAFE_CAST('{date}' AS DATE) AS collect_date
              )
            ]
          )
      WHEN sm.metrics IS NOT NULL AND m.metrics IS NULL THEN sm.metrics
      ELSE ARRAY_CONCAT(m.metrics, sm.metrics)
    END AS metrics
  FROM `{project_id}.{bigqury_dataset}.movies` AS m
  FULL JOIN `{project_id}.{bigqury_dataset}.staging_movies` AS sm
  ON m.movie_id = sm.movie_id
) u
ON t.movie_id = u.movie_id
WHEN MATCHED THEN
UPDATE SET
  t.opening_gross = u.opening_gross,
  t.movie_budget = u.movie_budget,
  t.metrics = u.metrics
WHEN NOT MATCHED THEN
    INSERT (
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
        metrics
    )
    VALUES (
        u.movie_id,
        u.imdb_id,
        u.original_title,
        u.title,
        u.genres,
        u.origin_country,
        u.production_companies,
        u.release_date,
        u.runtime,
        u.casts,
        u.crews,
        u.opening_gross,
        u.movie_budget,
        u.metrics
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