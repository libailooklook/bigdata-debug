package org.apache.flink.connector.elasticsearch.debug.es7.source;

import org.apache.flink.connector.elasticsearch.debug.base.ElasticsearchTestBase;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@SetEnvironmentVariable(key = "ES_SOURCE_HOST", value = "http://127.0.0.1:10600")
@SetEnvironmentVariable(key = "ES_SOURCE_USERNAME", value = "elastic")
@SetEnvironmentVariable(key = "ES_SOURCE_PASSWORD", value = "bigdata5214e")
@SetEnvironmentVariable(key = "ES_SINK_HOST", value = "http://127.0.0.1:10700")
@SetEnvironmentVariable(key = "ES_SINK_USERNAME", value = "elastic")
@SetEnvironmentVariable(key = "ES_SINK_PASSWORD", value = "bigdata5214e")
@SetEnvironmentVariable(key = "ES_DEBUG_SHARD_IDS", value = "34")
public class FlinkSqlElasticsearch7DebugSourceTests extends ElasticsearchTestBase {

    @BeforeAll
    public static void beforeAll() {
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source", Level.INFO);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.commons.StopWatch", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceReader", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.READER", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.RUNNING", Level.INFO);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.REQUEST", Level.DEBUG);
    }

    @BeforeEach
    public void beforeEach() {

    }

    private static final String RESOURCE_PATH = "elasticsearch/es7/source/";
    private static final int PARALLELISM = 1;
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(5);

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index") 
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    public void testUnboundedSequenceSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/unbounded_sequence_mode.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index") 
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    public void testBoundedSequenceSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/bounded_sequence_mode.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    public void testUnboundedSerialSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/unbounded_serial_mode.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    public void testBoundedSerialSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/bounded_serial_mode.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    @SetEnvironmentVariable(key = "ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS", value = "864000000") // 10 days
    public void testUnboundedTimelineSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/unbounded_timeline_mode.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_scan_index")
    @SetEnvironmentVariable(key = "ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS", value = "864000000") // 10 days
    public void testBoundedTimelineSource() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/bounded_timeline_mode.sql");
    }

}
