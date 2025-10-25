--This procedure allows idempotent incremental load of the tags table
CREATE OR REPLACE PROCEDURE `my_project.zoho_desk.spUpsertTagDetails`()
OPTIONS (strict_mode=false)

BEGIN

MERGE `my_project.zoho_desk.salesiq_conversation_tags` AS T
USING (
  SELECT *
  FROM `my_project.zoho_desk.salesiq_conversation_tagsDelta`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY chat_id
    ORDER BY end_time DESC NULLS LAST, start_time DESC NULLS LAST
  ) = 1
) AS S
ON T.chat_id = S.chat_id

WHEN MATCHED THEN UPDATE SET
  T.system_chat_id = S.system_chat_id,
  T.type           = S.type,
  T.start_time     = S.start_time,
  T.end_time       = S.end_time,
  T.phone          = S.phone,
  T.email          = S.email,
  T.tags           = S.tags,  -- JSON
  T.ticket_id      = S.ticket_id 

WHEN NOT MATCHED THEN INSERT (
  chat_id,
  system_chat_id,
  type,
  start_time,
  end_time,
  phone,
  email,
  tags,       -- JSON
  ticket_id
) VALUES (
  S.chat_id,
  S.system_chat_id,
  S.type,
  S.start_time,
  S.end_time,
  S.phone,
  S.email,
  S.tags,
  S.ticket_id
);
END;