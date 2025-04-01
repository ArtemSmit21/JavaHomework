CREATE KEYSPACE my_keyspace
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 3
        };
CREATE TABLE my_keyspace.user_audit (
    user_id UUID,
    event_time TIMESTAMP,
    event_type TEXT,
    event_details TEXT,
    PRIMARY KEY ( (user_id), event_time )
) WITH CLUSTERING ORDER BY (event_time DESC )
   AND DEFAULT_TIME_TO_LIVE = 31536000;