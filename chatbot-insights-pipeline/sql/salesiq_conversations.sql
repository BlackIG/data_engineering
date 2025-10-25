-- BQ SQL
-- Partition by chat start time and cluster by a few query filters

-- Replace `my_project.zoho_desk.salesiq_conversationDelta` with your staging table
CREATE TABLE `my_project.zoho_desk.salesiq_conversationDelta` (
  chat_id           STRING,
  chat_start_time   TIMESTAMP,
  chat_end_time     TIMESTAMP,
  operating_system  STRING,
  first_message     STRING,
  visitor_name      STRING,
  visitor_email     STRING,
  happiness_rating  INT64,
  feedback          STRING,
  chat_closed_by    STRING,
  chat              JSON,
  run_timestamp     TIMESTAMP
)
PARTITION BY DATE(chat_start_time)
CLUSTER BY operating_system, chat_closed_by;


-- Replace `my_project.zoho_desk.salesiq_conversation` with your target. Partition expiration is optional
CREATE TABLE `my_project.zoho_desk.salesiq_conversation` (
  chat_id                       STRING,
  chat_start_time               TIMESTAMP,
  chat_end_time                 TIMESTAMP,
  operating_system              STRING,
  first_message                 STRING,
  visitor_name                  STRING,
  visitor_email                 STRING,
  happiness_rating              INT64,
  feedback                      STRING,
  chat_closed_by                STRING,
  chat                          STRING,
  run_timestamp                 TIMESTAMP,
  contains_unmatched_response   INT64,
  asked_to_speak_with_agent     INT64
)
PARTITION BY DATE(chat_start_time)
CLUSTER BY operating_system, chat_closed_by
OPTIONS (
  partition_expiration_days = 30,
  require_partition_filter = TRUE
);
