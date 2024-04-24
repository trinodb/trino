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
package io.trino.tests.product.hive;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestComments
        extends ProductTest
{
    private static final String COMMENT_TABLE_NAME = "comment_test";
    private static final String COMMENT_VIEW_NAME = "comment_view_test";
    private static final String COMMENT_COLUMN_NAME = "comment_column_test";
    private static final String COMMENT_HIVE_VIEW_NAME = "comment_hive_view_test";

    @BeforeMethodWithContext
    @AfterMethodWithContext
    public void dropTestTable()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + COMMENT_TABLE_NAME);
        onTrino().executeQuery("DROP VIEW IF EXISTS " + COMMENT_VIEW_NAME);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + COMMENT_COLUMN_NAME);
        onHive().executeQuery("DROP VIEW IF EXISTS " + COMMENT_HIVE_VIEW_NAME);
    }

    @Test
    public void testCommentTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'old comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                COMMENT_TABLE_NAME);
        onTrino().executeQuery(createTableSql);

        assertThat(getTableComment("hive", "default", COMMENT_TABLE_NAME)).isEqualTo("old comment");

        onTrino().executeQuery(format("COMMENT ON TABLE %s IS 'new comment'", COMMENT_TABLE_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_TABLE_NAME)).isEqualTo("new comment");

        onTrino().executeQuery(format("COMMENT ON TABLE %s IS ''", COMMENT_TABLE_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_TABLE_NAME)).isEmpty();

        onTrino().executeQuery(format("COMMENT ON TABLE %s IS NULL", COMMENT_TABLE_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_TABLE_NAME)).isNull();
    }

    @Test
    public void testCommentView()
    {
        String createViewSql = format("" +
                        "CREATE VIEW hive.default.%s " +
                        "COMMENT 'old comment' " +
                        "AS SELECT 1 AS col",
                COMMENT_VIEW_NAME);
        onTrino().executeQuery(createViewSql);

        assertThat(getTableComment("hive", "default", COMMENT_VIEW_NAME)).isEqualTo("old comment");

        onTrino().executeQuery(format("COMMENT ON VIEW %s IS 'new comment'", COMMENT_VIEW_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_VIEW_NAME)).isEqualTo("new comment");

        onTrino().executeQuery(format("COMMENT ON VIEW %s IS ''", COMMENT_VIEW_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_VIEW_NAME)).isEmpty();

        onTrino().executeQuery(format("COMMENT ON VIEW %s IS NULL", COMMENT_VIEW_NAME));
        assertThat(getTableComment("hive", "default", COMMENT_VIEW_NAME)).isNull();

        onTrino().executeQuery(format("CREATE TABLE hive.default.%s (col int)", COMMENT_TABLE_NAME));
        onHive().executeQuery(format("CREATE VIEW default.%s AS SELECT * FROM default.%s", COMMENT_HIVE_VIEW_NAME, COMMENT_TABLE_NAME));
        assertThatThrownBy(() -> onTrino().executeQuery(format("COMMENT ON VIEW %s IS NULL", COMMENT_HIVE_VIEW_NAME)))
                .hasMessageContaining("Hive views are not supported");
    }

    private static String getTableComment(String catalogName, String schemaName, String tableName)
    {
        String sql = "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'";
        return (String) onTrino().executeQuery(sql).getOnlyValue();
    }

    @Test
    public void testCommentViewColumn()
    {
        String columnName = "col";
        String createViewSql = format("" +
                        "CREATE VIEW hive.default.%s " +
                        "AS SELECT 1 AS %s",
                COMMENT_VIEW_NAME,
                columnName);
        onTrino().executeQuery(createViewSql);

        assertThat(getColumnComment("default", COMMENT_VIEW_NAME, columnName)).isNull();

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.%s IS 'new col comment'", COMMENT_VIEW_NAME, columnName));
        assertThat(getColumnComment("default", COMMENT_VIEW_NAME, columnName)).isEqualTo("new col comment");

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.%s IS ''", COMMENT_VIEW_NAME, columnName));
        assertThat(getColumnComment("default", COMMENT_VIEW_NAME, columnName)).isEmpty();

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.%s IS NULL", COMMENT_VIEW_NAME, columnName));
        assertThat(getColumnComment("default", COMMENT_VIEW_NAME, columnName)).isNull();

        onTrino().executeQuery(format("CREATE TABLE hive.default.%s (col int)", COMMENT_TABLE_NAME));
        onHive().executeQuery(format("CREATE VIEW default.%s AS SELECT * FROM default.%s", COMMENT_HIVE_VIEW_NAME, COMMENT_TABLE_NAME));
        assertThatThrownBy(() -> onTrino().executeQuery(format("COMMENT ON COLUMN %s.%s IS NULL", COMMENT_HIVE_VIEW_NAME, columnName)))
                .hasMessageContaining("Hive views are not supported");
    }

    protected String getColumnComment(String schemaName, String tableName, String columnName)
    {
        String sql = "SELECT comment FROM information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'";
        return (String) onTrino().executeQuery(sql).getOnlyValue();
    }

    @Test
    public void testCommentColumn()
    {
        onTrino().executeQuery("""
                CREATE TABLE hive.default.%s (
                   c1 bigint COMMENT 'test comment',
                   c2 bigint COMMENT '',
                   c3 bigint,
                   c4 bigint COMMENT 'test partition comment',
                   c5 bigint COMMENT '',
                   c6 bigint
                )
                WITH (
                   partitioned_by = ARRAY['c4', 'c5', 'c6']
                )
                """.formatted(COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c1")).isEqualTo("test comment");
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c2")).isEmpty();
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c3")).isNull();
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c4")).isEqualTo("test partition comment");
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c5")).isEmpty();
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c6")).isNull();

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS 'new comment'", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c1")).isEqualTo("new comment");
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c4 IS 'new partition comment'", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c4")).isEqualTo("new partition comment");

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS ''", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c1")).isEmpty();
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c4 IS ''", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c4")).isEmpty();

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS NULL", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c1")).isNull();
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c4 IS NULL", COMMENT_COLUMN_NAME));
        assertThat(getColumnComment("default", COMMENT_COLUMN_NAME, "c4")).isNull();
    }
}
