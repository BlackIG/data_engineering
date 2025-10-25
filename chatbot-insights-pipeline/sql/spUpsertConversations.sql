--This procedure allows idempotent incremental load of the conversations table

CREATE OR REPLACE PROCEDURE `my_project.zoho_desk.spUpsertConversations`()
OPTIONS (strict_mode=false, description="Upsert into final from delta with HTML/JSON cleaning")
BEGIN

-- Helper
-- Clean HTML helper (for all text fields including chat)
CREATE TEMP FUNCTION clean_html(s STRING)
RETURNS STRING
AS (
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(s, r'&#39;|&#x27;|&#x2019;|&apos;', "'"),
          r'&quot;', '"'
        ),
        r'&amp;', '&'
      ),
      r'&nbsp;|&#160;|\x{A0}', ' '
    ),
    r'\s+', ' '
  )
);
-- Upsert
MERGE `my_project.zoho_desk.salesiq_conversations` AS T
USING (
  -- Build cleaned source inside USING(...)
  WITH src AS (
    SELECT *
    FROM `my_project.zoho_desk.salesiq_conversationDelta`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY chat_id
      ORDER BY run_timestamp DESC
    ) = 1
  ),
  s AS (
    SELECT
      chat_id,
      chat_start_time,
      chat_end_time,
      operating_system,
      first_message  AS first_message,
      visitor_name    AS visitor_name,
      visitor_email   AS visitor_email,
      happiness_rating,
      feedback      AS feedback,
      chat_closed_by  AS chat_closed_by,
      clean_html(TO_JSON_STRING(chat)) AS chat,  -- store as clean string
      run_timestamp
    FROM src
  )
  SELECT * FROM s
) AS S
ON T.chat_id = S.chat_id

WHEN MATCHED THEN UPDATE SET
  T.chat_start_time  = S.chat_start_time,
  T.chat_end_time    = S.chat_end_time,
  T.operating_system = S.operating_system,
  T.first_message    = S.first_message,
  T.visitor_name     = S.visitor_name,
  T.visitor_email    = S.visitor_email,
  T.happiness_rating = S.happiness_rating,
  T.feedback         = S.feedback,
  T.chat_closed_by   = S.chat_closed_by,
  T.chat             = S.chat,         -- cleaned JSON
  T.run_timestamp    = S.run_timestamp,
  T.contains_unmatched_response =  (CASE
                                      WHEN LOWER(S.chat) LIKE '%sorry%i%couldn%t%provide%any%'
  OR LOWER(S.chat) LIKE '%sorry%i%couldn%t%find%any%' THEN 1 ELSE 0 END),
  T.asked_to_speak_with_agent = (CASE
                                      WHEN LOWER(S.chat) LIKE '%transfer%chat%' THEN 1 ELSE 0 END)

WHEN NOT MATCHED THEN INSERT (
  chat_id,
  chat_start_time,
  chat_end_time,
  operating_system,
  first_message,
  visitor_name,
  visitor_email,
  happiness_rating,
  feedback,
  chat_closed_by,
  chat,
  run_timestamp,
  contains_unmatched_response,
  asked_to_speak_with_agent
) VALUES (
  S.chat_id,
  S.chat_start_time,
  S.chat_end_time,
  S.operating_system,
  S.first_message,
  S.visitor_name,
  S.visitor_email,
  S.happiness_rating,
  S.feedback,
  S.chat_closed_by,
  S.chat,
  S.run_timestamp,
  (CASE
                                      WHEN LOWER(S.chat) LIKE '%sorry%i%couldn%t%provide%any%'
  OR LOWER(S.chat) LIKE '%sorry%i%couldn%t%find%any%' THEN 1 ELSE 0 END),
  (CASE
                                      WHEN LOWER(S.chat) LIKE '%transfer%chat%' THEN 1 ELSE 0 END)

);

END;