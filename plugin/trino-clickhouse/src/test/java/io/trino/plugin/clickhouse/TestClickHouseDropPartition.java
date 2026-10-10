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

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_TABLE_PROCEDURE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestClickHouseDropPartition
        extends AbstractTestQueryFramework
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer)
                // The escaping test partitions by a String column, which is only comparable as text
                // when the connector reads it as varchar rather than varbinary.
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                .build();
    }

    @Test
    void testDropPartitionWithNumericKey()
    {
        try (TestTable table = createTable(
                "test_drop_partition_numeric",
                "(d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d",
                ImmutableList.of("'2020-01-15', 1", "'2020-02-15', 2", "'2020-03-15', 3"))) {
            // the partition is named by the value of the partition expression, so '202001' rather
            // than the date itself or the partition id
            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('202001')".formatted(table.getName()));

            assertQuery("SELECT x FROM " + table.getName() + " ORDER BY x", "VALUES 2, 3");
        }
    }

    @Test
    void testDropPartitionWithDateKey()
    {
        try (TestTable table = createTable(
                "test_drop_partition_date",
                "(d Date, x Int64) ENGINE = MergeTree PARTITION BY toDate(d) ORDER BY d",
                ImmutableList.of("'2020-01-15', 1", "'2020-01-16', 2"))) {
            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('2020-01-15')".formatted(table.getName()));

            assertQuery("SELECT x FROM " + table.getName() + " ORDER BY x", "VALUES 2");
        }
    }

    @Test
    void testDropPartitionEscapesThePartitionValue()
    {
        try (TestTable table = createTable(
                "test_drop_partition_escape",
                "(s String, x Int64) ENGINE = MergeTree PARTITION BY s ORDER BY s",
                ImmutableList.of("'a''b', 1", "'c', 2"))) {
            // A quote in the partition value has to survive as data rather than end the literal
            // that the procedure builds.
            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('a''b')".formatted(table.getName()));

            assertQuery("SELECT s FROM " + table.getName(), "VALUES 'c'");
        }
    }

    @Test
    void testDropPartitionIsIdempotent()
    {
        try (TestTable table = createTable(
                "test_drop_partition_idempotent",
                "(d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d",
                ImmutableList.of("'2020-01-15', 1"))) {
            // ClickHouse treats dropping a partition that does not exist as a no-op, and the
            // procedure passes that through so that it stays safe to run more than once
            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('209901')".formatted(table.getName()));
            assertQuery("SELECT x FROM " + table.getName(), "VALUES 1");

            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('202001')".formatted(table.getName()));
            assertQueryReturnsEmptyResult("SELECT x FROM " + table.getName());

            assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('202001')".formatted(table.getName()));
            assertQueryReturnsEmptyResult("SELECT x FROM " + table.getName());
        }
    }

    @Test
    void testDropPartitionWithCompositeKey()
    {
        try (TestTable table = createTable(
                "test_drop_partition_composite",
                "(d Date, x Int64) ENGINE = MergeTree PARTITION BY (toYYYYMM(d), x) ORDER BY d",
                ImmutableList.of("'2020-01-15', 1"))) {
            // A composite partition key takes a tuple, which a single value cannot express. This
            // pins the limitation documented for the procedure.
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE %s EXECUTE DROP_PARTITION('202001')".formatted(table.getName())))
                    .hasMessageContaining("Wrong number of fields in the partition expression");
        }
    }

    @Test
    void testDropPartitionRequiresExecutePrivilege()
    {
        try (TestTable table = createTable(
                "test_drop_partition_privilege",
                "(d Date, x Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d",
                ImmutableList.of("'2020-01-15', 1"))) {
            // ALTER TABLE ... EXECUTE names the table, so the engine can check the privilege for it,
            // which is why the procedure is not exposed through CALL.
            String catalog = getSession().getCatalog().orElseThrow();
            assertAccessDenied(
                    "ALTER TABLE %s EXECUTE DROP_PARTITION('202001')".formatted(table.getName()),
                    "Cannot execute table procedure DROP_PARTITION on %s.%s".formatted(catalog, table.getName()),
                    privilege("%s.%s.DROP_PARTITION".formatted(catalog, table.getName()), EXECUTE_TABLE_PROCEDURE));

            assertQuery("SELECT x FROM " + table.getName(), "VALUES 1");
        }
    }

    private TestTable createTable(String namePrefix, String tableDefinition, List<String> rowsToInsert)
    {
        // The tables are created with raw SQL because the connector's partition_by property quotes
        // each element as a column name, so it cannot express a partition key like toYYYYMM(d).
        return new TestTable(clickhouseServer::execute, "tpch." + namePrefix, tableDefinition, rowsToInsert);
    }
}
