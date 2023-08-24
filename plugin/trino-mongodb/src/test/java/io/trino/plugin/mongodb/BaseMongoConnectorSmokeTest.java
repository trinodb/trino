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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseMongoConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_MERGE,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testProjectionPushdown()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_multiple_rows_",
                "(id INT, nested1 ROW(child1 INT, child2 VARCHAR))",
                ImmutableList.of(
                        "(1, ROW(10, 'a'))",
                        "(2, ROW(NULL, 'b'))",
                        "(3, ROW(30, 'c'))",
                        "(4, NULL)"))) {
            assertThat(query("SELECT id, nested1.child1 FROM " + testTable.getName() + " WHERE nested1.child2 = 'c'"))
                    .matches("VALUES (3, 30)")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testReadDottedField()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_read_dotted_field_",
                "(root ROW(\"dotted.field\" VARCHAR, field VARCHAR))",
                ImmutableList.of("ROW(ROW('foo', 'bar'))"))) {
            assertThat(query("SELECT root.\"dotted.field\" FROM " + testTable.getName()))
                    .matches("SELECT varchar 'foo'");

            assertThat(query("SELECT root.\"dotted.field\", root.field FROM " + testTable.getName()))
                    .matches("SELECT varchar 'foo', varchar 'bar'");
        }
    }

    @Test
    public void testReadDollarPrefixedField()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_read_dotted_field_",
                "(root ROW(\"$field1\" VARCHAR, field2 VARCHAR))",
                ImmutableList.of("ROW(ROW('foo', 'bar'))"))) {
            assertThat(query("SELECT root.\"$field1\" FROM " + testTable.getName()))
                    .matches("SELECT varchar 'foo'");

            assertThat(query("SELECT root.\"$field1\", root.field2 FROM " + testTable.getName()))
                    .matches("SELECT varchar 'foo', varchar 'bar'");
        }
    }

    @Test
    public void testProjectionPushdownWithHighlyNestedData()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_highly_nested_data_",
                "(id INT, row1_t ROW(f1 INT, f2 INT, row2_t ROW (f1 INT, f2 INT, row3_t ROW(f1 INT, f2 INT))))",
                ImmutableList.of("(1, ROW(2, 3, ROW(4, 5, ROW(6, 7))))",
                        "(11, ROW(12, 13, ROW(14, 15, ROW(16, 17))))",
                        "(21, ROW(22, 23, ROW(24, 25, ROW(26, 27))))"))) {
            // Test select projected columns, with and without their parent column
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f2 FROM " + testTable.getName(), "VALUES (1, 7), (11, 17), (21, 27)");
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f2, CAST(row1_t AS JSON) FROM " + testTable.getName(),
                    "VALUES (1, 7, '{\"f1\":2,\"f2\":3,\"row2_t\":{\"f1\":4,\"f2\":5,\"row3_t\":{\"f1\":6,\"f2\":7}}}'), " +
                            "(11, 17, '{\"f1\":12,\"f2\":13,\"row2_t\":{\"f1\":14,\"f2\":15,\"row3_t\":{\"f1\":16,\"f2\":17}}}'), " +
                            "(21, 27, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");

            // Test predicates on immediate child column and deeper nested column
            assertQuery("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 = 27", "VALUES (21, '{\"f1\":26,\"f2\":27}')");
            assertQuery("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 > 20", "VALUES (21, '{\"f1\":26,\"f2\":27}')");
            assertQuery("SELECT id, CAST(row1_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 = 27",
                    "VALUES (21, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");
            assertQuery("SELECT id, CAST(row1_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 > 20",
                    "VALUES (21, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");

            // Test predicates on parent columns
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t = ROW(16, 17)", "VALUES (11, 16)");
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + testTable.getName() + " WHERE row1_t = ROW(22, 23, ROW(24, 25, ROW(26, 27)))", "VALUES (21, 26)");
        }
    }
}
