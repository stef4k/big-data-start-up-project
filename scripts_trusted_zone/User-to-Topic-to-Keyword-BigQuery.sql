-- Build the user–topic–keywords table, sampling 10 topics & 500 keywords each
CREATE OR REPLACE TABLE
  `calm-depot-454710-p0.landing_eusw1.user_topic_keywords` AS

WITH
  -- users
  users AS (
    SELECT
      COALESCE(CAST(__key__.id AS STRING), GENERATE_UUID()) AS user_id,
      __key__.name AS name
    FROM
      `calm-depot-454710-p0.landing_eusw1.public_users`
  ),

  -- all (topic, keyword) pairs from your 4 sources
  all_topic_keywords AS (
    SELECT topic, kw
    FROM (
      SELECT topic, kw FROM `calm-depot-454710-p0.landing_eusw1.reddit_posts`,      UNNEST(keywords) AS kw
      UNION ALL
      SELECT topic, kw FROM `calm-depot-454710-p0.landing_eusw1.stackexchange_posts`, UNNEST(keywords) AS kw
      UNION ALL
      SELECT topic, kw FROM `calm-depot-454710-p0.landing_eusw1.trivia_questions`,    UNNEST(keywords) AS kw
      UNION ALL
      SELECT topic, kw FROM `calm-depot-454710-p0.landing_eusw1.wikipedia_questions`, UNNEST(keywords) AS kw
    )
  ),

  -- dedupe
  topic_kw AS (
    SELECT DISTINCT topic, kw
    FROM all_topic_keywords
  ),

  -- all distinct topics
  all_topics AS (
    SELECT DISTINCT topic FROM topic_kw
  ),

  -- sample up to 10 topics per user
  user_topics AS (
    SELECT
      u.user_id,
      u.name,
      t.topic
    FROM
      users u
      CROSS JOIN (
        SELECT topic
        FROM all_topics
        ORDER BY RAND()
        LIMIT 10
      ) AS t
  ),

  -- join back to get every (user, topic, kw), then rank each keyword randomly per user+topic
  ranked_kw AS (
    SELECT
      ut.user_id,
      ut.name,
      ut.topic,
      tk.kw,
      ROW_NUMBER() OVER (
        PARTITION BY ut.user_id, ut.topic
        ORDER BY FARM_FINGERPRINT(CONCAT(ut.user_id, tk.kw))
      ) AS rn
    FROM
      user_topics ut
      JOIN topic_kw tk
      USING (topic)
  ),

  -- keep only the top 750 keywords per (user, topic)
  filtered AS (
    SELECT
      user_id,
      name,
      topic,
      kw
    FROM ranked_kw
    WHERE rn <= 500
  )

-- aggregate into an ARRAY<STRING>
SELECT
  user_id,
  name,
  topic,
  ARRAY_AGG(kw ORDER BY kw) AS keywords
FROM filtered
GROUP BY user_id, name, topic;

EXPORT DATA
  OPTIONS (
    uri='gs://group-1-delta-lake-lets-talk/trusted_zone/user_topic_keywords/*.parquet',
    format='PARQUET',
    overwrite=true
  ) AS
SELECT * FROM calm-depot-454710-p0.landing_eusw1.user_topic_keywords;
