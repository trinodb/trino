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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableType;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toUnmodifiableList;

public class TestDatabricksWithGlueMetastoreCleanUp
        extends ProductTest
{
    private static final Logger log = Logger.get(TestDatabricksWithGlueMetastoreCleanUp.class);
    private static final Instant SCHEMA_CLEANUP_THRESHOLD = Instant.now().minus(7, ChronoUnit.DAYS);
    private static final long MAX_JOB_TIME_MILLIS = MINUTES.toMillis(5);

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCleanUpOldTablesUsingDelta()
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.standard().build();
        long startTime = currentTimeMillis();
        List<String> schemas = onTrino().executeQuery("SELECT DISTINCT(table_schema) FROM information_schema.tables")
                .rows().stream()
                .map(row -> (String) row.get(0))
                .filter(schema -> schema.toLowerCase(ENGLISH).startsWith("test") || schema.equals("default"))
                .collect(toUnmodifiableList());

        // this is needed to make deletion of some views possible
        onTrino().executeQuery("SET SESSION hive.hive_views_legacy_translation = true");
        schemas.forEach(schema -> cleanSchema(schema, startTime, glueClient));
    }

    private void cleanSchema(String schema, long startTime, AWSGlueAsync glueClient)
    {
        Set<String> allTestTableNames = findAllTablesInSchema(schema).stream()
                .filter(name -> name.toLowerCase(ENGLISH).startsWith("test"))
                .collect(toImmutableSet());
        log.info("Found %d tables to drop in schema %s", allTestTableNames.size(), schema);
        int droppedTablesCount = 0;
        for (String tableName : allTestTableNames) {
            try {
                Table table = glueClient.getTable(new GetTableRequest().withDatabaseName(schema).withName(tableName)).getTable();
                Instant createTime = table.getCreateTime().toInstant();
                if (createTime.isBefore(SCHEMA_CLEANUP_THRESHOLD)) {
                    if (getTableType(table).contains("VIEW")) {
                        onTrino().executeQuery(format("DROP VIEW IF EXISTS %s.%s", schema, tableName));
                        log.info("Dropped view %s.%s", schema, tableName);
                    }
                    else {
                        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s.%s", schema, tableName));
                        log.info("Dropped table %s.%s", schema, tableName);
                    }
                    droppedTablesCount++;
                }
                if (currentTimeMillis() - startTime > MAX_JOB_TIME_MILLIS) {
                    break;
                }
            }
            catch (Exception e) {
                log.warn(e, "Exception while dropping table %s.%s", schema, tableName);
            }
        }
        log.info("Dropped %d tables in schema %s", droppedTablesCount, schema);
        if (!schema.equals("default") && findAllTablesInSchema(schema).isEmpty()) {
            try {
                onTrino().executeQuery("DROP SCHEMA IF EXISTS " + schema);
                log.info("Dropped schema %s", schema);
            }
            catch (Exception e) {
                log.warn(e, "Tried to delete schema %s but failed", schema);
            }
        }
    }

    private Set<String> findAllTablesInSchema(String schema)
    {
        try {
            QueryResult allTables = onTrino().executeQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema));
            return allTables.rows().stream()
                    .map(row -> (String) row.get(0))
                    .collect(Collectors.toUnmodifiableSet());
        }
        catch (Exception e) {
            log.warn(e, "Exception while fetching tables for schema %s", schema);
            return ImmutableSet.of();
        }
    }
}
