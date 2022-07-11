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

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.COMMENT;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestComments
        extends ProductTest
{
    private static final String COMMENT_TABLE_NAME = "comment_test";
    private static final String COMMENT_COLUMN_NAME = "comment_column_test";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTable()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + COMMENT_TABLE_NAME);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + COMMENT_COLUMN_NAME);
    }

    @Test(groups = COMMENT)
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

    private static String getTableComment(String catalogName, String schemaName, String tableName)
    {
        String sql = "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'";
        QueryResult result = onTrino().executeQuery(sql);
        return (String) result.row(0).get(0);
    }

    @Test(groups = COMMENT)
    public void testCommentColumn()
    {
        String createTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT 'test comment',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                COMMENT_COLUMN_NAME);
        onTrino().executeQuery(createTableSql);

        String createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT 'test comment',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);
        QueryResult actualResult = onTrino().executeQuery("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT 'new comment',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS 'new comment'", COMMENT_COLUMN_NAME));
        actualResult = onTrino().executeQuery("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT '',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS ''", COMMENT_COLUMN_NAME));
        actualResult = onTrino().executeQuery("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c1 IS NULL", COMMENT_COLUMN_NAME));
        actualResult = onTrino().executeQuery("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);
    }
}
