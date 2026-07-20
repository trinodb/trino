/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;

import static io.trino.client.TrinoJsonCodec.singlePassQueryResultsCodec;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Measures decoding a full QueryResults response of the direct protocol into rows, using
 * the single-pass codec configuration of {@link StatementClientV1}. Responses generated
 * with dataBeforeColumns exercise the buffering fallback of {@link QueryDataJacksonModule}
 * instead, so that parameter compares single-pass decoding against buffering. Run with the
 * GC profiler to also compare allocation per decoded page.
 */
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 700, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 700, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkQueryResultsDecoding
{
    private static final TrinoJsonCodec<QueryResults> QUERY_RESULTS_CODEC = singlePassQueryResultsCodec(false);

    public enum Shape
    {
        MIXED,
        NUMERIC,
        VARCHAR,
    }

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param("10000")
        public int rows = 10_000;

        @Param({"MIXED", "NUMERIC", "VARCHAR"})
        public Shape shape = Shape.MIXED;

        @Param({"false", "true"})
        public boolean dataBeforeColumns;

        public String response;
        public ResultRowsDecoder decoder;

        @Setup
        public void setup()
        {
            response = buildResponse();
            decoder = new ResultRowsDecoder();
        }

        @TearDown
        public void tearDown()
                throws Exception
        {
            decoder.close();
        }

        private String buildResponse()
        {
            StringBuilder columns = new StringBuilder("[");
            StringBuilder data = new StringBuilder("[");
            switch (shape) {
                case MIXED:
                    columns.append(column("c1", "bigint")).append(",").append(column("c2", "varchar")).append(",")
                            .append(column("c3", "double")).append(",").append(column("c4", "boolean")).append(",")
                            .append(column("c5", "timestamp(3)")).append(",").append(column("c6", "integer")).append(",")
                            .append(column("c7", "varchar")).append(",").append(column("c8", "bigint"));
                    for (int i = 0; i < rows; i++) {
                        if (i > 0) {
                            data.append(",");
                        }
                        data.append("[").append(i)
                                .append(",\"customer_name_").append(i).append("_with_some_padding\"")
                                .append(",").append(i * 0.5)
                                .append(",").append(i % 2 == 0)
                                .append(",\"2026-07-17 10:11:12.").append(100 + i % 900).append("\"")
                                .append(",").append(i % 1000)
                                .append(",\"region_").append(i % 50).append("\"")
                                .append(",").append(i * 1_000_000L)
                                .append("]");
                    }
                    break;
                case NUMERIC:
                    columns.append(column("c1", "bigint")).append(",").append(column("c2", "integer")).append(",")
                            .append(column("c3", "double")).append(",").append(column("c4", "double")).append(",")
                            .append(column("c5", "bigint")).append(",").append(column("c6", "boolean"));
                    for (int i = 0; i < rows; i++) {
                        if (i > 0) {
                            data.append(",");
                        }
                        data.append("[").append(i).append(",").append(i % 1000).append(",")
                                .append(i * 0.25).append(",").append(i * 123.456).append(",")
                                .append(i * 7_000_000L).append(",").append(i % 3 == 0).append("]");
                    }
                    break;
                case VARCHAR:
                    columns.append(column("c1", "varchar")).append(",").append(column("c2", "varchar")).append(",")
                            .append(column("c3", "varchar")).append(",").append(column("c4", "varchar"));
                    for (int i = 0; i < rows; i++) {
                        if (i > 0) {
                            data.append(",");
                        }
                        data.append("[\"some_reasonably_long_string_value_number_").append(i).append("\"")
                                .append(",\"another_padded_column_value_").append(i).append("_tail\"")
                                .append(",\"third_column_with_padding_").append(i % 97).append("\"")
                                .append(",\"fourth_column_final_value_").append(i % 13).append("\"]");
                    }
                    break;
            }
            columns.append("]");
            data.append("]");

            String stats = "{\"state\": \"FINISHED\", \"queued\": false, \"scheduled\": false, \"nodes\": 1, "
                    + "\"totalSplits\": 10, \"queuedSplits\": 0, \"runningSplits\": 0, \"completedSplits\": 10, "
                    + "\"cpuTimeMillis\": 100, \"wallTimeMillis\": 100, \"queuedTimeMillis\": 1, \"elapsedTimeMillis\": 101, "
                    + "\"processedRows\": " + rows + ", \"processedBytes\": 1000000, \"peakMemoryBytes\": 1000}";

            String columnsAndData = "\"columns\": " + columns + ", \"data\": " + data;
            if (dataBeforeColumns) {
                columnsAndData = "\"data\": " + data + ", \"columns\": " + columns;
            }
            return "{\"id\": \"20260717_000000_00000_aaaaa\", "
                    + "\"infoUri\": \"http://localhost:8080/query.html?20260717_000000_00000_aaaaa\", "
                    + "\"nextUri\": \"http://localhost:8080/v1/statement/executing/20260717_000000_00000_aaaaa/xxx/1\", "
                    + columnsAndData + ", "
                    + "\"stats\": " + stats + "}";
        }

        private static String column(String name, String type)
        {
            String rawType = type.replaceAll("\\(.*\\)", "");
            return "{\"name\": \"" + name + "\", \"type\": \"" + type + "\", \"typeSignature\": {\"rawType\": \"" + rawType + "\", \"arguments\": []}}";
        }
    }

    @Benchmark
    public long decode(BenchmarkData data)
            throws Exception
    {
        QueryResults results = QUERY_RESULTS_CODEC.fromJson(data.response);
        long count = 0;
        try (ResultRows resultRows = data.decoder.toRows(results)) {
            for (List<Object> row : resultRows) {
                count += row.size();
            }
        }
        return count;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        new Runner(new OptionsBuilder()
                .include(BenchmarkQueryResultsDecoding.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build()).run();
    }
}
