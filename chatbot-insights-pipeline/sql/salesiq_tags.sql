-- BQ SQL

--Replace `my_project.zoho_desk.salesiq_conversation_tagsDelta` with your staging table
CREATE TABLE `my_project.zoho_desk.salesiq_conversation_tagsDelta` (
  chat_id       STRING,
  system_chat_id STRING,
  type          STRING,
  start_time    TIMESTAMP,
  end_time      TIMESTAMP,
  phone         STRING,
  email         STRING,
  tags          JSON,
  ticket_id     STRING
);

-- Create target table, copy only schema, partition expiration is optional
CREATE TABLE `my_project.zoho_desk.salesiq_conversation_tags`
PARTITION BY DATE(start_time)
OPTIONS (
  partition_expiration_days = 30,
  require_partition_filter = TRUE
);
AS SELECT * FROM `my_project.zoho_desk.salesiq_conversation_tagsDelta`
WHERE 1=0