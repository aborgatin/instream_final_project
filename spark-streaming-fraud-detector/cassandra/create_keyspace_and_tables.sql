CREATE KEYSPACE clicks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE clicks.user_clicks (
    ip text,
    event_id uuid,
    event_type text,
    is_bot boolean,
    url text,
    event_time timestamp,
    PRIMARY KEY (ip, event_time, event_id)
);


