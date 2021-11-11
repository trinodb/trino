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

import java.util.Optional;

import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.COMMENT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestComments
        extends ProductTest
{
    private static final String COMMENT_TABLE_NAME = "comment_test";
    private static final String COMMENT_COLUMN_NAME = "comment_column_test";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTable()
    {
        query("DROP TABLE IF EXISTS " + COMMENT_TABLE_NAME);
        query("DROP TABLE IF EXISTS " + COMMENT_COLUMN_NAME);
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
        query(createTableSql);

        QueryResult actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(tableWithCommentPattern(COMMENT_TABLE_NAME, Optional.of("old comment")));

        query(format("COMMENT ON TABLE %s IS 'new comment'", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(tableWithCommentPattern(COMMENT_TABLE_NAME, Optional.of("new comment")));

        query(format("COMMENT ON TABLE %s IS ''", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(tableWithCommentPattern(COMMENT_TABLE_NAME, Optional.of("")));

        query(format("COMMENT ON TABLE %s IS NULL", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(tableWithCommentPattern(COMMENT_TABLE_NAME, Optional.empty()));
    }

    private String tableWithCommentPattern(String tableName, Optional<String> expectedComment)
    {
        return String.format("CREATE TABLE hive.default.\\Q%s\\E \\((?s:[^)]+)\\)\n", tableName) +
                expectedComment.map(comment -> "COMMENT '" + comment + "'\n").orElse("") +
                "WITH(?s:.*)";
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
        query(createTableSql);

        String createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT 'test comment',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);
        QueryResult actualResult = query("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT 'new comment',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        query(format("COMMENT ON COLUMN %s.c1 IS 'new comment'", COMMENT_COLUMN_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint COMMENT '',\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        query(format("COMMENT ON COLUMN %s.c1 IS ''", COMMENT_COLUMN_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);

        createTableSqlPattern = format("\\Q" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 bigint COMMENT '',\n" +
                        "   c3 bigint\n" +
                        ")\\E(?s:.*)",
                COMMENT_COLUMN_NAME);

        query(format("COMMENT ON COLUMN %s.c1 IS NULL", COMMENT_COLUMN_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_COLUMN_NAME);
        assertThat((String) actualResult.row(0).get(0)).matches(createTableSqlPattern);
    }
}
