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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD) // Run sequentially to remove tests interference, as both tests flush the cache
final class TestBigQueryStaleNameResolution
        extends AbstractTestQueryFramework
{
    private final BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();

    private BigQueryClientFactory queryClientFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.case-insensitive-name-matching", "true")
                        .put("bigquery.case-insensitive-name-matching.cache-ttl", "5m")
                        .put("bigquery.metadata.cache-ttl", "5m")
                        // Prevent the client, which holds the caches, from expiring during a test
                        .put("bigquery.service-cache-ttl", "5m")
                        .buildOrThrow())
                .build();
        Connector connector = queryRunner.getCoordinator().getConnector("bigquery");
        queryClientFactory = ((BigQueryConnector) connector).getInjector().getInstance(BigQueryClientFactory.class);
        return queryRunner;
    }

    @Test
    void testTableAmbiguityClearedAfterCollidingTableDropped()
            throws Exception
    {
        String schemaName = "test_stale_ambiguous_table_" + randomNameSuffix();
        try (AutoCloseable _ = withSchemaCreatedInBigQuery(schemaName);
                TestTable table = new TestTable(bigQuerySqlExecutor, schemaName + ".Test_Table", "(c string)")) {
            String tableName = table.getName().split("\\.")[1];
            String tableNameUpperCase = tableName.toUpperCase(ENGLISH);
            String select = "SELECT * FROM %s.%s".formatted(schemaName, tableName.toLowerCase(ENGLISH));

            assertThat(computeActual(select)).isEmpty();

            // Flush so the collision is observed and the ambiguity cached
            bigQuerySqlExecutor.execute(format("CREATE TABLE %s.%s (c string)", schemaName, tableNameUpperCase));
            queryClientFactory.flushCache();
            assertQueryFails(select, "Found ambiguous names in BigQuery when looking up '%s'.*".formatted(tableName.toLowerCase(ENGLISH)));

            // Drop directly in BigQuery, keeping the cached ambiguity
            bigQuerySqlExecutor.execute(format("DROP TABLE %s.%s", schemaName, tableNameUpperCase));

            assertThat(computeActual(select)).isEmpty();
        }
    }

    @Test
    void testSchemaAmbiguityClearedAfterCollidingSchemaDropped()
            throws Exception
    {
        String schemaNameLowerCase = "test_stale_ambiguous_schema_" + randomNameSuffix();
        String schemaNameUpperCase = schemaNameLowerCase.toUpperCase(ENGLISH);
        try (AutoCloseable _ = withSchemaCreatedInBigQuery(schemaNameLowerCase);
                TestTable table = new TestTable(bigQuerySqlExecutor, schemaNameLowerCase + ".test_table", "(c string)")) {
            String tableName = table.getName().split("\\.")[1];
            String select = "SELECT * FROM %s.%s".formatted(schemaNameLowerCase, tableName);

            assertThat(computeActual(select)).isEmpty();

            // Flush so the collision is observed and the ambiguity cached
            bigQuerySqlExecutor.execute("CREATE SCHEMA " + schemaNameUpperCase);
            queryClientFactory.flushCache();
            assertQueryFails(select, "Found ambiguous names in BigQuery when looking up '%s'.*".formatted(schemaNameLowerCase));

            // Drop directly in BigQuery, keeping the cached ambiguity
            bigQuerySqlExecutor.execute("DROP SCHEMA " + schemaNameUpperCase);

            assertThat(computeActual(select)).isEmpty();
        }
        finally {
            bigQuerySqlExecutor.execute("DROP SCHEMA IF EXISTS " + schemaNameUpperCase + " CASCADE");
        }
    }

    private AutoCloseable withSchemaCreatedInBigQuery(String schemaName)
    {
        bigQuerySqlExecutor.execute("CREATE SCHEMA " + schemaName);
        return () -> bigQuerySqlExecutor.dropDatasetIfExists(schemaName);
    }
}
