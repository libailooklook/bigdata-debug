create temporary table source (
       `id` bigint,
       `update_time` timestamp,
       `es_id`   varchar(20) METADATA from '_id' VIRTUAL,
       `es_seq_no`   bigint METADATA from '_seq_no' VIRTUAL
) with (
       'connector' = 'elasticsearch-7-ce',
       'hosts' = '${ES_SOURCE_HOST}',
       'username' = '${ES_SOURCE_USERNAME}',
       'password' = '${ES_SOURCE_PASSWORD}',
       'index' = '${ES_SOURCE_INDEX}',
       'scan.mode' = 'sequence',
       'scan.sequence.starting.strategy' = 'specified',
       'scan.sequence.stopping.strategy' = 'specified',
       'scan.sequence.starting.offset' = '1',
       'scan.sequence.stopping.offset' = '1000000',
       'scan.stopping.check-interval-millis' = '300000',
       'scan.sort.fields' = '_seq_no,_doc',
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
       'connector' = 'elasticsearch-7-ce',
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