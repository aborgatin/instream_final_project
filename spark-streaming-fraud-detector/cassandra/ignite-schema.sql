CREATE TABLE bots (
  ip varchar PRIMARY KEY, last_event_time timestamp)
  WITH "template=replicated";

CREATE INDEX idx_bots_last_event_time ON bots (last_event_time);