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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toUnmodifiableList;

public class TestDeltaLakeDatabricksCompatibilityCleanUp
        extends ProductTest
{
    private static final Logger log = Logger.get(TestDeltaLakeDatabricksCompatibilityCleanUp.class);
    private static final long SCHEMA_CLEANUP_THRESHOLD = DAYS.toMillis(7);
    private static final long MAX_JOB_TIME = MINUTES.toMillis(5);
    private static final Set<String> SCHEMAS_TO_SKIP = ImmutableSet.of("information_schema", "tpch", "tcpds", "sf1", "sf10", "sf100", "sf1000", "tpcds_sf1", "tpcds_sf10", "tpcds_sf100", "tpcds_sf1000", "tpcds_sf300");

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testCleanUpOldTablesUsingDelta()
    {
        long startTime = currentTimeMillis();
        List<String> schemas = onTrino().executeQuery("SELECT DISTINCT(table_schema) FROM information_schema.tables")
                .rows().stream()
                .map(row -> (String) row.get(0))
                .filter(schema -> SCHEMAS_TO_SKIP.stream().noneMatch(schema::equalsIgnoreCase))
                .collect(toUnmodifiableList());
        schemas.forEach(schema -> cleanSchema(schema, startTime));
    }

    private void cleanSchema(String schema, long startTime)
    {
        List<List<?>> allTables = findAllTablesInSchema(schema);
        int numberOfTablesInTheSchema = allTables.size();
        int droppedTablesCount = 0;
        for (List<?> row : allTables) {
            String tableName = (String) row.get(0);
            try {
                // delete tables that are older than the SCHEMA_CLEANUP_THRESHOLD
                // if table data was deleted from its location but table was not dropped its creationTime points to currentTime
                // to find this tables we obtain their creation time twice and compare it, if it changes it means table needs to be dropped
                List<List<?>> details1 = onDelta().executeQuery(format("DESCRIBE DETAIL %s.%s", schema, tableName)).rows();
                long createdAt1 = ((Timestamp) details1.get(0).get(5)).getTime();
                List<List<?>> details2 = onDelta().executeQuery(format("DESCRIBE DETAIL %s.%s", schema, tableName)).rows();
                long createdAt2 = ((Timestamp) details2.get(0).get(5)).getTime();
                if ((createdAt1 != createdAt2) || createdAt1 <= SCHEMA_CLEANUP_THRESHOLD) {
                    onDelta().executeQuery(format("DROP TABLE IF EXISTS %s.%s", schema, tableName));
                    log.info("Dropped table %s", tableName);
                    droppedTablesCount++;
                }
                if (currentTimeMillis() - startTime > MAX_JOB_TIME) {
                    break;
                }
            }
            catch (Exception e) {
                log.warn(e, "Exception while dropping table %s", tableName);
            }
        }
        log.debug("Dropped %d tables in schema %s", droppedTablesCount, schema);
        if (droppedTablesCount == numberOfTablesInTheSchema && !schema.equals("default")) {
            try {
                onTrino().executeQuery("DROP SCHEMA IF EXISTS " + schema);
                log.info("Dropped schema %s", schema);
            }
            catch (Exception e) {
                log.warn(e, "Tried to delete schema %s but failed", schema);
            }
        }
    }

    private List<List<?>> findAllTablesInSchema(String schema)
    {
        try {
            QueryResult allTables = onTrino().executeQuery(format("SHOW TABLES IN %s", schema));
            log.debug("Found %d potential tables to drop in schema %s", allTables.rows().size(), schema);
            return allTables.rows();
        }
        catch (Exception e) {
            log.warn(e, "Exception while fetching tables for schema %s", schema);
            return ImmutableList.of();
        }
    }
}
