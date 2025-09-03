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
package io.trino.plugin.pinot;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.pinot.PinotQueryRunner.PINOT_CATALOG;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.join;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotConnectorTest
        extends BaseConnectorTest
{
    private static final String OFFLINE_INSERT_TABLE = "offline_insert";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        kafka.start();
        TestingPinotCluster pinot = closeAfterClass(new TestingPinotCluster(PINOT_LATEST_IMAGE_NAME, kafka.getNetwork(), false, false, Optional.empty()));
        pinot.start();
        createOfflineInsert(pinot);

        return PinotQueryRunner.builder()
                .setKafka(kafka)
                .setPinot(pinot)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override // Override because updated_at_seconds column exists
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("updated_at_seconds", "bigint", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "date", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("totalprice", "double", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertEqualsIgnoreOrder(computeActual("SHOW COLUMNS FROM orders"), getDescribeOrdersResult());
    }

    @Test
    @Override
    public void testSelectAll()
    {
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM orders");
    }

    @Override
    protected @Language("SQL") String getOrdersTableWithColumns()
    {
        return """
               VALUES
                   ('orders', 'orderkey'),
                   ('orders', 'custkey'),
                   ('orders', 'orderstatus'),
                   ('orders', 'totalprice'),
                   ('orders', 'orderdate'),
                   ('orders', 'updated_at_seconds'),
                   ('orders', 'orderpriority'),
                   ('orders', 'clerk'),
                   ('orders', 'shippriority'),
                   ('orders', 'comment')
               """;
    }

    @Test
    @Override // Override because updated_at_seconds column exists
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(
                        """
                        CREATE TABLE pinot.default.orders (
                           clerk varchar,
                           comment varchar,
                           custkey bigint,
                           orderdate date,
                           orderkey bigint,
                           orderpriority varchar,
                           orderstatus varchar,
                           shippriority integer,
                           totalprice double,
                           updated_at_seconds bigint
                        )""");
    }

    @Test
    @Override // Override because the regexp is different from the base test
    public void testPredicateReflectedInExplain()
    {
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "columnName=nationkey", "dataType=bigint", "\\s\\{\\[42\\]\\}");
    }

    @Test
    @Override
    public void testInsert()
    {
        String insertedNullValues = "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, bigint '31536000', NULL, NULL)";
        assertUpdate(getInsertStatement(OFFLINE_INSERT_TABLE, getColumnList(), getValuesClause(List.of(insertedNullValues))), 1);
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        String insertedNegativeTimestampValues = "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, bigint '-31536000', NULL, NULL)";
        assertThatThrownBy(() -> getQueryRunner().execute(
                getInsertStatement(OFFLINE_INSERT_TABLE, getColumnList(), getValuesClause(List.of(insertedNegativeTimestampValues)))))
                .hasMessageContaining("Invalid segment start/end time")
                .hasMessageContaining("must be between");
    }

    private List<String> getColumnList()
    {
        return List.of(
            "string_col",
            "bool_col",
            "bytes_col",
            "string_array_col",
            "bool_array_col",
            "int_array_col",
            "int_array_col_with_pinot_default",
            "float_array_col",
            "double_array_col",
            "long_array_col",
            "timestamp_col",
            "timestamp_array_col",
            "json_col",
            "int_col",
            "float_col",
            "double_col",
            "long_col",
            "updated_at_seconds",
            "updated_at_hours",
            "ts");
    }

    private static String getValuesClause(List<String>... values)
    {
        return "VALUES " + join(",\n", Arrays.stream(values).flatMap(List::stream).collect(toUnmodifiableList()));
    }

    private static String getInsertStatement(String tableName, List<String> columnNames, String valuesClause)
    {
        return "INSERT INTO " + PINOT_CATALOG + ".default." + tableName + "(" + getColumnListSql(columnNames) + ")\n  " + valuesClause;
    }

    private static String getColumnListSql(List<String> columnNames)
    {
        return join(", ", columnNames);
    }

    private void createOfflineInsert(TestingPinotCluster pinot)
            throws Exception
    {
        pinot.createSchema("offline_insert_schema.json", OFFLINE_INSERT_TABLE);
        pinot.addOfflineTable("offline_insert_spec.json", OFFLINE_INSERT_TABLE);
    }
}
