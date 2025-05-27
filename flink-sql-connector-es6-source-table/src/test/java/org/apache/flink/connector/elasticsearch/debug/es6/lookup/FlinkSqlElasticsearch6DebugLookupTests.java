package org.apache.flink.connector.elasticsearch.debug.es6.lookup;

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

@SetEnvironmentVariable(key = "ES_SOURCE_HOST", value = "http://172.20.12.6:10600")
@SetEnvironmentVariable(key = "ES_SOURCE_USERNAME", value = "elastic")
@SetEnvironmentVariable(key = "ES_SOURCE_PASSWORD", value = "bigdata5214e")
@SetEnvironmentVariable(key = "ES_DIM_HOST", value = "http://127.0.0.1:10600")
@SetEnvironmentVariable(key = "ES_DIM_USERNAME", value = "elastic")
@SetEnvironmentVariable(key = "ES_DIM_PASSWORD", value = "bigdata5214e")
@SetEnvironmentVariable(key = "ES_SINK_HOST", value = "http://127.0.0.1:10600")
@SetEnvironmentVariable(key = "ES_SINK_USERNAME", value = "elastic")
@SetEnvironmentVariable(key = "ES_SINK_PASSWORD", value = "bigdata5214e")
public class FlinkSqlElasticsearch6DebugLookupTests extends ElasticsearchTestBase {

    @BeforeAll
    public static void beforeAll() {
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source", Level.INFO);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.commons.StopWatch", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceReader", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.READER", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.RUNNING", Level.INFO);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceShardReader.REQUEST", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.ElasticsearchLookup.READER", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.ElasticsearchLookup.RUNNING", Level.INFO);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.ElasticsearchLookup.REQUEST", Level.DEBUG);
        Configurator.setLevel("org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchLookupFullCacheLoader.REQUEST", Level.INFO);
    }

    @BeforeEach
    public void beforeEach() {

    }

    private static final String RESOURCE_PATH = "elasticsearch/es6/lookup/";
    private static final int PARALLELISM = 1;
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(5);

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS", value = "864000000") // 10 days
    @SetEnvironmentVariable(key = "ES_DIM_INDEX", value = "test_scan_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_lookup_index")
    public void testLookupNoCache() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/lookup_no_cache.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS", value = "864000000") // 10 days
    @SetEnvironmentVariable(key = "ES_DIM_INDEX", value = "test_scan_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_lookup_index")
    public void testLookupPartialCache() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/lookup_partial_cache.sql");
    }

    @Test
    @SetEnvironmentVariable(key = "ES_SOURCE_INDEX", value = "test_source_index")
    @SetEnvironmentVariable(key = "ES_SOURCE_STOPPING_OFFSET_INTERVAL_MILLIS", value = "864000000") // 10 days
    @SetEnvironmentVariable(key = "ES_DIM_INDEX", value = "test_scan_index")
    @SetEnvironmentVariable(key = "ES_SINK_INDEX", value = "test_lookup_index")
    public void testLookupFullCache() throws ExecutionException, InterruptedException, IOException {
        execute(PARALLELISM, CHECKPOINT_INTERVAL, RESOURCE_PATH + "/lookup_full_cache.sql");
    }

}
