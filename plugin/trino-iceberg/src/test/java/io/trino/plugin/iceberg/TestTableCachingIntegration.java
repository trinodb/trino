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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.manager.TableCachingPredicate;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableCachingIntegration
        extends AbstractTestQueryFramework
{
    private static final String CACHED_SCHEMA = "cached_schema";
    private static final String NON_CACHED_SCHEMA = "non_cached_schema";
    private TableCachingPredicate cachingPredicate;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        cachingPredicate = new TableCachingPredicate(ImmutableList.of("cached_schema.*", "prod.orders"));

        Path cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));
        Path metastoreDirectory = Files.createTempDirectory(ICEBERG_CATALOG);
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("fs.cache.include-tables", "cached_schema.*,prod.orders")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .buildOrThrow();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(CACHED_SCHEMA)
                        .build())
                .setIcebergProperties(icebergProperties)
                .setWorkerCount(0)
                .build();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + CACHED_SCHEMA);
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + NON_CACHED_SCHEMA);
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS prod");
        return queryRunner;
    }

    @Test
    public void testCachingPredicateMatchesSchemaWildcard()
    {
        assertThat(cachingPredicate.test(new SchemaTableName("cached_schema", "any_table"))).isTrue();
        assertThat(cachingPredicate.test(new SchemaTableName("cached_schema", "orders"))).isTrue();
        assertThat(cachingPredicate.test(new SchemaTableName("cached_schema", "customers"))).isTrue();
    }

    @Test
    public void testCachingPredicateMatchesExactTable()
    {
        assertThat(cachingPredicate.test(new SchemaTableName("prod", "orders"))).isTrue();
        assertThat(cachingPredicate.test(new SchemaTableName("prod", "customers"))).isFalse();
    }

    @Test
    public void testCachingPredicateDoesNotMatchNonCachedSchema()
    {
        assertThat(cachingPredicate.test(new SchemaTableName("non_cached_schema", "orders"))).isFalse();
        assertThat(cachingPredicate.test(new SchemaTableName("dev", "some_table"))).isFalse();
    }

    @Test
    public void testCreateAndQueryTableInCachedSchema()
    {
        assertUpdate("CREATE TABLE cached_schema.test_orders (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO cached_schema.test_orders VALUES (1, 'order1'), (2, 'order2')", 2);

        assertQuery("SELECT * FROM cached_schema.test_orders ORDER BY id", "VALUES (1, 'order1'), (2, 'order2')");
        assertQuery("SELECT count(*) FROM cached_schema.test_orders", "VALUES 2");

        assertThat(cachingPredicate.test(new SchemaTableName("cached_schema", "test_orders"))).isTrue();

        assertUpdate("DROP TABLE cached_schema.test_orders");
    }

    @Test
    public void testCreateAndQueryTableInNonCachedSchema()
    {
        assertUpdate("CREATE TABLE non_cached_schema.temp_data (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO non_cached_schema.temp_data VALUES (10, 'value1'), (20, 'value2')", 2);

        assertQuery("SELECT * FROM non_cached_schema.temp_data ORDER BY id", "VALUES (10, 'value1'), (20, 'value2')");
        assertQuery("SELECT count(*) FROM non_cached_schema.temp_data", "VALUES 2");

        assertThat(cachingPredicate.test(new SchemaTableName("non_cached_schema", "temp_data"))).isFalse();

        assertUpdate("DROP TABLE non_cached_schema.temp_data");
    }

    @Test
    public void testQueryTablesAcrossSchemas()
    {
        assertUpdate("CREATE TABLE cached_schema.data_a (id BIGINT)");
        assertUpdate("CREATE TABLE non_cached_schema.data_b (id BIGINT)");

        assertUpdate("INSERT INTO cached_schema.data_a VALUES (1), (2), (3)", 3);
        assertUpdate("INSERT INTO non_cached_schema.data_b VALUES (4), (5)", 2);

        assertQuery("SELECT * FROM cached_schema.data_a ORDER BY id", "VALUES 1, 2, 3");
        assertQuery("SELECT * FROM non_cached_schema.data_b ORDER BY id", "VALUES 4, 5");

        assertThat(cachingPredicate.test(new SchemaTableName("cached_schema", "data_a"))).isTrue();
        assertThat(cachingPredicate.test(new SchemaTableName("non_cached_schema", "data_b"))).isFalse();

        assertUpdate("DROP TABLE cached_schema.data_a");
        assertUpdate("DROP TABLE non_cached_schema.data_b");
    }
}
