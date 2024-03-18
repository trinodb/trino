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

package io.trino.tests.product.warp;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.WarmUtils;
import org.intellij.lang.annotations.Language;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.EXTERNAL_COLLECT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.EXTERNAL_MATCH;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.VARADA_COLLECT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.VARADA_MATCH;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.ROW_GROUP_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARMUP_ELEMENTS_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_FAILED;
import static io.trino.tests.product.warp.utils.TestUtils.getFullyQualifiedName;
import static io.trino.tests.product.warp.utils.TestUtils.getSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class WarpSpeedTests
{
    private static final Logger logger = Logger.get(WarpSpeedTests.class);

    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    DemoterUtils demoterUtils;

    public WarpSpeedTests()
    {
    }

    public void testCleanup(String schemaName, String tableName, String[] columnNames)
    {
        try {
            demoterUtils.demote(schemaName, tableName, Arrays.stream(columnNames).toList(), -0.99, 0, true, true, false);
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
        catch (Exception e) {
            logger.error(e, "demote failed");
        }
    }

    public void testWarpSimpleQuery(String catalogName, String schemaName, String testName)
    {
        String tableName = getFullyQualifiedName(catalogName, schemaName, "nation") + getSuffix();

        try {
            onTrino().executeQuery(format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM tpch.tiny.nation", tableName));

            @Language("SQL") String query = format("SELECT * FROM %s WHERE nationkey > 1", tableName);
            Map<String, Long> expectedWarmResults = Map.of(
                    WARM_ACCOMPLISHED, 1L,
                    WARMUP_ELEMENTS_COUNT, 5L,
                    ROW_GROUP_COUNT, 1L,
                    WARM_FAILED, 0L);
            warmUtils.warmAndValidate(query, expectedWarmResults);
            Map<String, Long> expectedQueryResults = Map.of(
                    VARADA_COLLECT, 4L,
                    VARADA_MATCH, 1L,
                    EXTERNAL_COLLECT, 0L,
                    EXTERNAL_MATCH, 0L);
            queryUtils.queryAndValidate(query, expectedQueryResults, testName);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));

            String[] columnNames = new String[] {"name", "nationkey", "regionkey", "comment"};
            testCleanup(schemaName, tableName.substring(tableName.lastIndexOf('.') + 1), columnNames);
        }
    }

    public void testWarpBucketedBy(String catalogName, String schemaName, String testName)
    {
        String tableName = getFullyQualifiedName(catalogName, schemaName, "bucketed_by") + getSuffix();

        try {
            onTrino().executeQuery(format("CREATE TABLE IF NOT EXISTS %s (" +
                    "col_id integer, " +
                    "col_tinyint tinyint, " +
                    "col_smallint smallint, " +
                    "col_int integer, " +
                    "col_bigint bigint, " +
                    "col_varchar varchar, " +
                    "col_char9 char(9), " +
                    "col_date date, " +
                    "col_timestamp timestamp) " +
                    "WITH (format = 'PARQUET', bucketed_by = ARRAY['col_id'], bucket_count = 3)", tableName));

            IntStream.range(1, 11).forEach(value -> onTrino().executeQuery(format("INSERT INTO %s VALUES (%d, %d, %d, %d, %d, '%d', '%09d', last_day_of_month(from_unixtime(%d)), from_unixtime(%d))",
                    tableName, value, value % 128, value % 4096, value, value, value, value, value, value)));

            Map<String, Long> expectedWarmResults = Map.of(
                    WARM_ACCOMPLISHED, 10L,
                    WARMUP_ELEMENTS_COUNT, 100L,
                    ROW_GROUP_COUNT, 10L,
                    WARM_FAILED, 0L);
            @Language("SQL") String query = format("SELECT * FROM %s WHERE col_id > 0", tableName);
            QueryResult queryResult = warmUtils.warmAndValidate(query, expectedWarmResults);

            assertThat(queryResult.getRowsCount())
                    .describedAs(format("row count=%d is not as expected=%d", queryResult.getRowsCount(), 10))
                    .isEqualTo(10);

            Map<String, Long> expectedQueryResults = Map.of(
                    VARADA_COLLECT, 90L,
                    VARADA_MATCH, 10L,
                    EXTERNAL_COLLECT, 0L,
                    EXTERNAL_MATCH, 0L);
            queryResult = queryUtils.queryAndValidate(query, expectedQueryResults, testName);

            assertThat(queryResult.getRowsCount())
                    .describedAs(format("row count=%d is not as expected=%d", queryResult.getRowsCount(), 10))
                    .isEqualTo(10);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));

            String[] columnNames = new String[] {"col_id", "col_tinyint", "col_smallint", "col_int", "col_bigint", "col_varchar", "col_char9", "col_date", "col_timestamp"};
            testCleanup(schemaName, tableName.substring(tableName.lastIndexOf('.') + 1), columnNames);
        }
    }
}
