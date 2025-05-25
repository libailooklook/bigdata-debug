create temporary table source (
       `id` bigint,
       `update_time` timestamp,
       `es_id`   varchar(20) METADATA from '_id' VIRTUAL,
       `es_seq_no`   bigint METADATA from '_seq_no' VIRTUAL
) with (
       'connector' = 'elasticsearch-7-scan-ce',
       'hosts' = '${ES_SOURCE_HOST}',
       'username' = '${ES_SOURCE_USERNAME}',
       'password' = '${ES_SOURCE_PASSWORD}',
       'index' = '${ES_SOURCE_INDEX}',
       'scan.mode' = 'timeline',
       'scan.stopping.check-interval-millis' = '300000',
       'scan.timeline.field' = 'update_time',
       'scan.timeline.stopping.offset.interval-millis' = '${ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS}',
       'scan.sort.fields' = 'update_time,_doc',
       'scan.batch.size' = '10000',
       'scan.fetch.size' = '1000'
);

create temporary table sink (
       `id` bigint,
       `update_time` timestamp,
       `es_id`   varchar(20),
       `es_seq_no`   bigint,
       primary key(id) not enforced
) with (
       'connector' = 'elasticsearch-7-sink-ce',
       'hosts' = '${ES_SINK_HOST}',
       'username' = '${ES_SINK_USERNAME}',
       'password' = '${ES_SINK_PASSWORD}',
       'index' = '${ES_SINK_INDEX}'
);

insert into sink
select
    id, update_time, es_id, es_seq_no
from source
;