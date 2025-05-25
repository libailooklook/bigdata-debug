package org.apache.flink.connector.elasticsearch.debug.base;

import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticsearchTestBase {

    protected Map<String, String> loadEnvironmentVariables() throws IOException {
        Map<String, String> env = new HashMap<>();
        env.putAll(System.getenv());
        Properties properties = new Properties();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(".env");
        properties.load(in);
        properties.forEach((key, value) -> env.put(key.toString(), value.toString()));
        in.close();
        return env;
    }

    protected List<String> loadTestSqlList(String path) {
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("File not found: " + path);
            }

            List<String> sqls = new ArrayList<>();
            StringBuilder sql = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                sql.append((char) ch);
                if (ch == ';') {
                    sqls.add(sql.toString());
                    sql.setLength(0);
                }
            }
            return sqls;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void execute(int parallelism, Duration checkpointInterval, String sqlPath) throws ExecutionException, InterruptedException, IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism > 0 ? parallelism : 1);
        if (checkpointInterval != null) {
            env.enableCheckpointing(checkpointInterval.toMillis());
        }
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        List<String> sqls = loadTestSqlList(sqlPath);
        StringSubstitutor substitutor = new StringSubstitutor(loadEnvironmentVariables());
        substitutor.setEnableSubstitutionInVariables(true);
        for (String sql : sqls) {
            sql = sql.trim();
            if (sql.isEmpty()) {
                continue;
            }
            sql = substitutor.replace(sql);
            System.out.println("Executing SQL: " + sql);
            TableResult result = tableEnv.executeSql(sql);
            result.await();
        }
    }

}
