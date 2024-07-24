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

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Cost-based optimizers' behaviors are affected by the statistics returned by the Connectors. Here is to count the getTableStatistics calls
// when CBOs work with Iceberg Connector.
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergGetTableStatisticsOperations
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;
    private Path metastoreDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(testSession())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        metastoreDir = Files.createTempDirectory("test_iceberg_get_table_statistics_operations");
        queryRunner.installPlugin(new TestingIcebergPlugin(metastoreDir));
        queryRunner.createCatalog("iceberg", "iceberg", ImmutableMap.of());

        HiveMetastore metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        Database database = Database.builder()
                .setDatabaseName("tiny")
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        queryRunner.execute("CREATE TABLE iceberg.tiny.orders AS SELECT * FROM tpch.tiny.orders");
        queryRunner.execute("CREATE TABLE iceberg.tiny.lineitem AS SELECT * FROM tpch.tiny.lineitem");
        queryRunner.execute("CREATE TABLE iceberg.tiny.customer AS SELECT * FROM tpch.tiny.customer");

        return queryRunner;
    }

    @Test
    public void testTwoWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM iceberg.tiny.orders o, iceberg.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(2);
    }

    @Test
    public void testThreeWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM iceberg.tiny.customer c, iceberg.tiny.orders o, iceberg.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey AND c.custkey = o.custkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(3);
    }

    private void planDistributedQuery(@Language("SQL") String sql)
    {
        queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
    }

    private long getTableStatisticsMethodInvocations()
    {
        return queryRunner.getSpans().stream()
                .map(SpanData::getName)
                .filter(name -> name.equals("Metadata.getTableStatistics"))
                .count();
    }
}
