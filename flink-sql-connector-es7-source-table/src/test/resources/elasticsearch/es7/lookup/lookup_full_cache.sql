create
temporary table source (
       `id` bigint,
       `update_time` timestamp,
       `proc_time` as PROCTIME()
) with (
       'connector' = 'elasticsearch-7-ce',
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

create
temporary table dim (
       `id` bigint,
       `update_time` timestamp,
       `es_id`   varchar(20) METADATA from '_id' VIRTUAL,
       `es_seq_no`   bigint METADATA from '_seq_no' VIRTUAL,
       primary key(id) not enforced
) with (
       'connector' = 'elasticsearch-7-ce',
       'hosts' = '${ES_DIM_HOST}',
       'username' = '${ES_DIM_USERNAME}',
       'password' = '${ES_DIM_PASSWORD}',
       'lookup.cache' = 'FULL',
       'lookup.full-cache.sort.fields' = 'id,update_time',
       'lookup.full-cache.reload-strategy' = 'PERIODIC',
       'lookup.full-cache.periodic-reload.interval' = '10m',
       'lookup.full-cache.periodic-reload.schedule-mode' = 'FIXED_DELAY',
       'index' = '${ES_DIM_INDEX}'
);

create
temporary table sink (
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
       'index' = '${ES_SINK_INDEX}',
       'sink.partition-routing.fields' = 'id'
);

insert into sink
select t1.id, t2.update_time, t2.es_id, t2.es_seq_no
from source as t1
         left join dim FOR SYSTEM_TIME AS OF t1.proc_time AS t2 on t1.id = t2.id
where t2.id is not null
;

insert into sink
select t1.id, t2.update_time, t2.es_id, t2.es_seq_no
from source as t1
         left join dim FOR SYSTEM_TIME AS OF t1.proc_time AS t2 on t1.id = t2.id
where t2.id is not null
;