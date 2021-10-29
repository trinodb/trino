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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.TPCH_SCHEMA;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class TestBigQueryInstanceCleaner
{
    public static final Logger LOG = Logger.get(TestBigQueryInstanceCleaner.class);

    /**
     * List of table names that will not be dropped.
     */
    private static final Collection<String> tablesToKeep = TpchTable.getTables().stream()
            .map(TpchTable::getTableName)
            .map(s -> s.toLowerCase(Locale.ENGLISH))
            .collect(toUnmodifiableSet());

    // see https://cloud.google.com/bigquery/docs/information-schema-tables#tables_view for possible values
    public static final Collection<String> tableTypesToDrop = ImmutableList.of("BASE TABLE", "VIEW", "MATERIALIZED VIEW");

    private BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass
    public void setUp()
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
    }

    @Test
    public void cleanUpDatasets()
    {
        LOG.info("Identifying datasets to drop...");
        // Drop all datasets with our label created more than 24 hours ago
        List<String> selfCreatedDatasets = bigQuerySqlExecutor.getSelfCreatedDatasets();
        TableResult result = bigQuerySqlExecutor.executeQuery(format("" +
                        "SELECT schema_name " +
                        "FROM INFORMATION_SCHEMA.SCHEMATA " +
                        "WHERE datetime_diff(current_datetime(), datetime(creation_time), HOUR) > 24 " +
                        "AND schema_name IN (%s)",
                selfCreatedDatasets.stream().collect(joining("','", "'", "'"))));
        List<String> datasetsToDrop = Streams.stream(result.getValues())
                .map(fieldValues -> fieldValues.get("schema_name").getStringValue())
                .collect(toImmutableList());

        if (datasetsToDrop.isEmpty()) {
            LOG.info("Did not find any datasets to drop.");
            return;
        }

        LOG.info("Dropping %s datasets.", datasetsToDrop.size());
        LOG.info("Dropping: %s", datasetsToDrop);
        datasetsToDrop.forEach(dataset -> {
            LOG.info("Dropping dataset '%s' that contains tables: %s", dataset, bigQuerySqlExecutor.getTableNames(dataset));
            bigQuerySqlExecutor.dropDatasetIfExists(dataset);
        });
    }

    @Test(dataProvider = "cleanUpSchemasDataProvider")
    public void cleanUpTables(String schemaName)
    {
        logObjectsCount(schemaName);
        if (!tablesToKeep.isEmpty()) {
            LOG.info("Will not drop these tables: %s", join(", ", tablesToKeep));
        }

        LOG.info("Identifying tables to drop...");
        // Drop all tables created more than 24 hours ago
        TableResult result = bigQuerySqlExecutor.executeQuery(format("" +
                        "SELECT table_name, table_type " +
                        "FROM %s.INFORMATION_SCHEMA.TABLES " +
                        "WHERE datetime_diff(current_datetime(), datetime(creation_time), HOUR) > 24 " +
                        "AND table_name NOT IN (%s)" +
                        "AND table_type IN (%s)",
                quoted(schemaName),
                tablesToKeep.stream().collect(joining("','", "'", "'")),
                tableTypesToDrop.stream().collect(joining("','", "'", "'"))));
        List<Entry<String, String>> objectsToDrop = Streams.stream(result.getValues())
                .map(fieldValues -> new SimpleImmutableEntry<>(fieldValues.get("table_name").getStringValue(), fieldValues.get("table_type").getStringValue()))
                .collect(toImmutableList());

        if (objectsToDrop.isEmpty()) {
            LOG.info("Did not find any objects to drop.");
            return;
        }

        LOG.info("Dropping %s objects.", objectsToDrop.size());
        LOG.info("Dropping: %s", objectsToDrop.stream().map(Entry::getKey).collect(joining(", ")));
        objectsToDrop.forEach(entry -> {
            String dropStatement = getDropStatement(schemaName, entry.getKey(), entry.getValue());
            LOG.info("Executing: %s", dropStatement);
            bigQuerySqlExecutor.execute(dropStatement);
        });

        logObjectsCount(schemaName);
    }

    @DataProvider
    public static Object[][] cleanUpSchemasDataProvider()
    {
        // Other schemas created by tests are taken care of by cleanUpDatasets
        return new Object[][] {
                {TPCH_SCHEMA},
                {TEST_SCHEMA},
        };
    }

    private void logObjectsCount(String schemaName)
    {
        TableResult result = bigQuerySqlExecutor.executeQuery(format("" +
                        "SELECT table_type, count(*) AS c " +
                        "FROM %s.INFORMATION_SCHEMA.TABLES " +
                        "GROUP BY table_type",
                quoted(schemaName)));
        result.getValues().forEach(fieldValues -> LOG.info("Schema '%s' contains '%s' objects of type '%s'",
                schemaName,
                fieldValues.get("c").getLongValue(),
                fieldValues.get("table_type").getStringValue()));
    }

    private static String getDropStatement(String schemaName, String objectName, String objectType)
    {
        switch (objectType) {
            case "BASE TABLE":
                return format("DROP TABLE IF EXISTS %s.%s", quoted(schemaName), quoted(objectName));
            case "VIEW":
                return format("DROP VIEW IF EXISTS %s.%s", quoted(schemaName), quoted(objectName));
            case "MATERIALIZED VIEW":
                return format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", quoted(schemaName), quoted(objectName));
            default:
                throw new IllegalArgumentException("Unexpected object type " + objectType);
        }
    }

    private static String quoted(String identifier)
    {
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }
}
