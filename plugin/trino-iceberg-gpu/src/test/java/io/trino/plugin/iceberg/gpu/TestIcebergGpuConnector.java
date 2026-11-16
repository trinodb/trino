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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.LINE_ITEM;

/**
 * The {@code iceberg_gpu} connector answers every query as the Iceberg connector does over the same
 * tables. Without a GPU, as here, it takes nothing over and is the Iceberg connector; what this
 * checks is that wrapping it changes no result, write or metadata.
 */
final class TestIcebergGpuConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(LINE_ITEM))
                .build();
        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        queryRunner.installPlugin(new IcebergGpuPlugin());
        queryRunner.createCatalog("iceberg_gpu", "iceberg_gpu", ImmutableMap.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", "local:///iceberg",
                "fs.native-local.enabled", "true",
                "local.location", dataDirectory.toString()));
        return queryRunner;
    }

    @Test
    void testAggregations()
    {
        assertSameResult(
                """
                SELECT returnflag, linestatus, sum(quantity), sum(extendedprice * (1 - discount)), avg(discount), count(*)
                FROM %s.tpch.lineitem
                WHERE shipdate <= DATE '1998-09-02'
                GROUP BY returnflag, linestatus
                """);
        assertSameResult(
                """
                SELECT sum(extendedprice * discount)
                FROM %s.tpch.lineitem
                WHERE shipdate >= DATE '1994-01-01' AND shipdate < DATE '1995-01-01' AND discount BETWEEN 0.05 AND 0.07 AND quantity < 24
                """);
        assertSameResult("SELECT linenumber, count(*), avg(tax) FROM %s.tpch.lineitem WHERE quantity BETWEEN 10 AND 20 GROUP BY linenumber");
        assertSameResult("SELECT count(*) FROM %s.tpch.lineitem");
    }

    @Test
    void testQueriesTheDeviceDoesNotTake()
    {
        assertSameResult("SELECT returnflag, min(shipdate), max(extendedprice) FROM %s.tpch.lineitem GROUP BY returnflag");
        assertSameResult("SELECT shipinstruct, count(*) FROM %s.tpch.lineitem GROUP BY shipinstruct");
        assertSameResult("SELECT count(*), sum(extendedprice) FROM %s.tpch.lineitem WHERE comment LIKE '%%special%%'");
        assertSameResult("SELECT orderkey, linenumber FROM %s.tpch.lineitem WHERE quantity > 49 ORDER BY orderkey, linenumber LIMIT 5");
    }

    @Test
    void testWritesThroughTheConnector()
    {
        assertUpdate("CREATE TABLE iceberg_gpu.tpch.written AS SELECT orderkey, quantity, extendedprice FROM tpch.tiny.lineitem", count("SELECT count(*) FROM tpch.tiny.lineitem"));
        assertUpdate("DELETE FROM iceberg_gpu.tpch.written WHERE orderkey % 7 = 0", count("SELECT count(*) FROM tpch.tiny.lineitem WHERE orderkey % 7 = 0"));
        assertUpdate("INSERT INTO iceberg_gpu.tpch.written SELECT orderkey, quantity, extendedprice FROM tpch.tiny.lineitem WHERE orderkey < 100", count("SELECT count(*) FROM tpch.tiny.lineitem WHERE orderkey < 100"));
        assertSameResult("SELECT sum(quantity), count(*) FROM %s.tpch.written WHERE extendedprice > 1000");
        assertUpdate("DROP TABLE iceberg_gpu.tpch.written");
    }

    private long count(@Language("SQL") String query)
    {
        return (long) computeScalar(query);
    }

    private void assertSameResult(@Language("SQL") String query)
    {
        MaterializedResult gpu = computeActual(query.formatted("iceberg_gpu"));
        MaterializedResult iceberg = computeActual(query.formatted("iceberg"));
        assertEqualsIgnoreOrder(gpu.getMaterializedRows(), iceberg.getMaterializedRows());
    }
}
