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
package io.trino.plugin.clickhouse;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_TABLE_PROCEDURE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestClickHouseDropPartition
        extends AbstractTestQueryFramework
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                .build();
    }

    @Test
    public void testDropPartitionWithNumericKey()
    {
        String tableName = tableName("numeric");
        // The tables are created with raw SQL because the connector's partition_by property quotes
        // each element as a column name, so it cannot express a partition key like toYYYYMM(d).
        executeOnClickHouse("CREATE TABLE tpch.%s (d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d".formatted(tableName));
        try {
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('2020-01-15', 1), ('2020-02-15', 2), ('2020-03-15', 3)".formatted(tableName));

            // the partition is named by the value of the partition expression, so '202001' rather
            // than the date itself or the partition id
            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('202001')", tableName));

            assertQuery("SELECT x FROM " + tableName + " ORDER BY x", "VALUES 2, 3");
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    @Test
    public void testDropPartitionWithDateKey()
    {
        String tableName = tableName("date");
        executeOnClickHouse("CREATE TABLE tpch.%s (d Date, x Int64) ENGINE = MergeTree PARTITION BY toDate(d) ORDER BY d".formatted(tableName));
        try {
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('2020-01-15', 1), ('2020-01-16', 2)".formatted(tableName));

            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('2020-01-15')", tableName));

            assertQuery("SELECT x FROM " + tableName + " ORDER BY x", "VALUES 2");
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    @Test
    public void testDropPartitionEscapesThePartitionValue()
    {
        String tableName = tableName("escape");
        executeOnClickHouse("CREATE TABLE tpch.%s (s String, x Int64) ENGINE = MergeTree PARTITION BY s ORDER BY s".formatted(tableName));
        try {
            // A quote in the partition value has to survive as data rather than end the literal
            // that the procedure builds.
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('a''b', 1), ('c', 2)".formatted(tableName));

            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('a''b')", tableName));

            assertQuery("SELECT s FROM " + tableName, "VALUES 'c'");
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    @Test
    public void testDropPartitionIsIdempotent()
    {
        String tableName = tableName("idempotent");
        executeOnClickHouse("CREATE TABLE tpch.%s (d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d".formatted(tableName));
        try {
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('2020-01-15', 1)".formatted(tableName));

            // ClickHouse treats dropping a partition that does not exist as a no-op, and the
            // procedure passes that through so that it stays safe to run more than once
            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('209901')", tableName));
            assertQuery("SELECT x FROM " + tableName, "VALUES 1");

            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('202001')", tableName));
            assertQueryReturnsEmptyResult("SELECT x FROM " + tableName);

            assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('202001')", tableName));
            assertQueryReturnsEmptyResult("SELECT x FROM " + tableName);
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    @Test
    public void testDropPartitionWithCompositeKey()
    {
        String tableName = tableName("composite");
        executeOnClickHouse("CREATE TABLE tpch.%s (d Date, x Int64) ENGINE = MergeTree PARTITION BY (toYYYYMM(d), x) ORDER BY d".formatted(tableName));
        try {
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('2020-01-15', 1)".formatted(tableName));

            // A composite partition key takes a tuple, which a single value cannot express. This
            // pins the limitation documented for the procedure.
            assertThatThrownBy(() -> assertUpdate(format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('202001')", tableName)))
                    .hasMessageContaining("Wrong number of fields in the partition expression");
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    @Test
    public void testDropPartitionRequiresExecutePrivilege()
    {
        String tableName = tableName("privilege");
        executeOnClickHouse("CREATE TABLE tpch.%s (d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d".formatted(tableName));
        try {
            executeOnClickHouse("INSERT INTO tpch.%s VALUES ('2020-01-15', 1)".formatted(tableName));

            // ALTER TABLE ... EXECUTE names the table, so the engine can check the privilege for it,
            // which is why the procedure is not exposed through CALL.
            String catalog = getSession().getCatalog().orElseThrow();
            assertAccessDenied(
                    format("ALTER TABLE tpch.%s EXECUTE DROP_PARTITION('202001')", tableName),
                    format("Cannot execute table procedure DROP_PARTITION on %s.tpch.%s", catalog, tableName),
                    privilege(format("%s.tpch.%s.DROP_PARTITION", catalog, tableName), EXECUTE_TABLE_PROCEDURE));

            assertQuery("SELECT x FROM " + tableName, "VALUES 1");
        }
        finally {
            executeOnClickHouse("DROP TABLE IF EXISTS tpch." + tableName);
        }
    }

    private void executeOnClickHouse(String sql)
    {
        clickhouseServer.execute(sql);
    }

    private static String tableName(String suffix)
    {
        return "test_drop_partition_%s_%s".formatted(suffix, randomNameSuffix());
    }
}
