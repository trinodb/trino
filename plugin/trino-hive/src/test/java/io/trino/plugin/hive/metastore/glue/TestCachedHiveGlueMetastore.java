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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_PARTITION_NAMES;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // glueStats is shared mutable state
public class TestCachedHiveGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 5;
    private final String testSchema = "test_schema_" + randomNameSuffix();

    private GlueMetastoreStats glueStats;
    private GlueClient glueClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .addHiveProperty("hive.metastore-cache-ttl", "1d")
                .addHiveProperty("hive.metastore-refresh-interval", "1h")
                .addHiveProperty("hive.security", "allow-all")
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema);
        glueStats = getConnectorService(queryRunner, GlueHiveMetastore.class).getStats();
        glueClient = closeAfterClass(GlueClient.create());
        return queryRunner;
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema + " CASCADE");
    }

    @Test
    public void testSelectUnpartitionedTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");
            String select = "SELECT * FROM test_select_from";
            // populate cache and verify test scaffolding (sanity check that getting counts works)
            assertInvocations(select, ImmutableMultiset.of(GET_TABLE));
            // cached
            assertInvocations(select, ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from");
        }
    }

    @Test
    public void testSelectPartitionedTable()
    {
        try {
            assertUpdate(
                    """
                            CREATE TABLE test_select_from_partitioned_where WITH (partitioned_by = ARRAY['regionkey']) AS
                            SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                            """,
                    25);
            String select = "SELECT * FROM test_select_from_partitioned_where WHERE regionkey IN (2, 3)";
            // populate cache and verify test scaffolding (sanity check that getting counts works)
            assertInvocations(select,
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .addCopies(GET_PARTITION_NAMES, 5)
                            .build());
            // cached
            assertInvocations(select, ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_partitioned_where");
        }
    }

    @Test
    public void testFlushTableCache()
            throws Exception
    {
        try {
            assertUpdate(
                    """
                            CREATE TABLE test_flush_table WITH (partitioned_by = ARRAY['regionkey']) AS
                            SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                            """,
                    25);
            String select = "SELECT * FROM test_flush_table WHERE regionkey IN (2, 3)";
            // populate cache
            assertQuerySucceeds(select);
            // cached
            assertInvocations(select, ImmutableMultiset.of());
            // delete partition behind the scenes
            glueClient.deletePartition(request -> request.databaseName(testSchema).tableName("test_flush_table").partitionValues("2"));
            Location partitionLocation = Location.of("local:///glue/" + testSchema + "/test_flush_table/regionkey=2");
            getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"))
                    .deleteDirectory(partitionLocation);
            // query fails because cache is not invalidated
            assertQueryFails(select, "Partition location does not exist: " + partitionLocation);
            // next query fails too, because query failure does not invalidate the cache
            assertQueryFails(select, "Partition location does not exist: " + partitionLocation);
            // flush cache
            assertQuerySucceeds("CALL system.flush_metadata_cache()");
            assertInvocations(select, ImmutableMultiset.<GlueMetastoreMethod>builder()
                    .add(GET_TABLE)
                    .addCopies(GET_PARTITION_NAMES, 5)
                    .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_partitioned_where");
        }
    }

    @Test
    public void testFlushPartitionCache()
            throws Exception
    {
        try {
            assertUpdate(
                    """
                            CREATE TABLE test_flush_partition WITH (partitioned_by = ARRAY['regionkey']) AS
                            SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                            """,
                    25);
            String select = "SELECT * FROM test_flush_partition WHERE regionkey IN (2, 3)";
            // populate cache
            assertQuerySucceeds(select);
            // cached
            assertInvocations(select, ImmutableMultiset.of());
            // delete partition behind the scenes
            glueClient.deletePartition(request -> request.databaseName(testSchema).tableName("test_flush_partition").partitionValues("2"));
            Location partitionLocation = Location.of("local:///glue/" + testSchema + "/test_flush_partition/regionkey=2");
            getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"))
                    .deleteDirectory(partitionLocation);
            // query fails because cache is not invalidated
            assertQueryFails(select, "Partition location does not exist: " + partitionLocation);
            // next query fails too, because query failure does not invalidate the cache
            assertQueryFails(select, "Partition location does not exist: " + partitionLocation);
            // flush cache
            assertQuerySucceeds("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_flush_partition', partition_columns => ARRAY['regionkey'], partition_values => ARRAY['2'])");
            assertQueryFails(select, "Partition no longer exists: regionkey=2");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_partitioned_where");
        }
    }

    private void assertInvocations(@Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        assertInvocations(getSession(), query, expectedGlueInvocations);
    }

    private void assertInvocations(Session session, @Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        Map<GlueMetastoreMethod, Integer> countsBefore = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        getQueryRunner().execute(session, query);

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        Multiset<GlueMetastoreMethod> actualGlueInvocations = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));

        assertMultisetsEqual(actualGlueInvocations, expectedGlueInvocations);
    }
}
