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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.Table;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.getTableType;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toUnmodifiableList;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDatabricksWithGlueMetastoreCleanUp
{
    private static final Logger log = Logger.get(TestDatabricksWithGlueMetastoreCleanUp.class);
    private static final Instant SCHEMA_CLEANUP_THRESHOLD = Instant.now().minus(7, ChronoUnit.DAYS);
    private static final long MAX_JOB_TIME_MILLIS = MINUTES.toMillis(5);

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCleanUpOldTablesUsingDelta(DeltaLakeDatabricksEnvironment env)
    {
        GlueClient glueClient = env.createGlueClient();
        long startTime = currentTimeMillis();
        List<String> schemas = env.executeTrinoSql("SELECT DISTINCT(table_schema) FROM hive.information_schema.tables")
                .rows().stream()
                .map(row -> (String) row.get(0))
                .filter(schema -> schema.toLowerCase(ENGLISH).startsWith("test") || schema.equals("default"))
                .collect(toUnmodifiableList());

        env.executeTrinoSql("SET SESSION hive.hive_views_legacy_translation = true");
        schemas.forEach(schema -> cleanSchema(env, schema, startTime, glueClient));
    }

    private void cleanSchema(DeltaLakeDatabricksEnvironment env, String schema, long startTime, GlueClient glueClient)
    {
        Database database;
        try {
            database = glueClient.getDatabase(builder -> builder.name(schema)).database();
        }
        catch (EntityNotFoundException _) {
            return;
        }

        if (database.createTime().isAfter(SCHEMA_CLEANUP_THRESHOLD)) {
            log.info("Skip dropping recently created schema %s", schema);
            return;
        }

        Set<String> allTestTableNames = findAllTablesInSchema(env, schema).stream()
                .filter(name -> name.toLowerCase(ENGLISH).startsWith("test"))
                .collect(toImmutableSet());
        log.info("Found %d tables to drop in schema %s", allTestTableNames.size(), schema);
        int droppedTablesCount = 0;
        for (String tableName : allTestTableNames) {
            try {
                Table table = glueClient.getTable(builder -> builder.databaseName(schema).name(tableName)).table();
                Instant createTime = table.createTime();
                if (createTime.isBefore(SCHEMA_CLEANUP_THRESHOLD)) {
                    if (getTableType(table).contains("VIEW")) {
                        env.executeTrinoSql(format("DROP VIEW IF EXISTS hive.%s.%s", schema, tableName));
                        log.info("Dropped view %s.%s", schema, tableName);
                    }
                    else {
                        env.executeTrinoSql(format("DROP TABLE IF EXISTS hive.%s.%s", schema, tableName));
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
        if (!schema.equals("default") && findAllTablesInSchema(env, schema).isEmpty()) {
            try {
                env.executeTrinoSql("DROP SCHEMA IF EXISTS hive." + schema);
                log.info("Dropped schema %s", schema);
            }
            catch (Exception e) {
                log.warn(e, "Tried to delete schema %s but failed", schema);
            }
        }
    }

    private Set<String> findAllTablesInSchema(DeltaLakeDatabricksEnvironment env, String schema)
    {
        try {
            QueryResult allTables = env.executeTrinoSql(format("SELECT table_name FROM hive.information_schema.tables WHERE table_schema = '%s'", schema));
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
