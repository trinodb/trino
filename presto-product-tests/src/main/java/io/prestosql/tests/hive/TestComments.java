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
package io.prestosql.tests.hive;

import io.airlift.log.Logger;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMENT;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestComments
        extends ProductTest
{
    private static final String COMMENT_TABLE_NAME = "comment_test";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTable()
    {
        try {
            query("DROP TABLE IF EXISTS " + COMMENT_TABLE_NAME);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
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
        assertShowCreateTableOutput(actualResult.row(0).get(0), createTableSql);

        String commentedCreateTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'new comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                COMMENT_TABLE_NAME);

        query(format("COMMENT ON TABLE %s IS 'new comment'", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertShowCreateTableOutput(actualResult.row(0).get(0), commentedCreateTableSql);

        commentedCreateTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT ''\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                COMMENT_TABLE_NAME);

        query(format("COMMENT ON TABLE %s IS ''", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertShowCreateTableOutput(actualResult.row(0).get(0), commentedCreateTableSql);

        commentedCreateTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                COMMENT_TABLE_NAME);

        query(format("COMMENT ON TABLE %s IS NULL", COMMENT_TABLE_NAME));
        actualResult = query("SHOW CREATE TABLE " + COMMENT_TABLE_NAME);
        assertShowCreateTableOutput(actualResult.row(0).get(0), commentedCreateTableSql);
    }

    private void assertShowCreateTableOutput(Object actual, String expected)
    {
        List<String> expectedLines = Stream.of(
                expected.split("\n"))
                .map(line -> line.lastIndexOf(',') == (line.length() - 1) ? line.substring(0, line.length() - 1) : line)
                .collect(Collectors.toList());

        List<String> absentLines = expectedLines.stream().filter(line -> !actual.toString().contains(line)).collect(Collectors.toList());

        assertTrue(absentLines.isEmpty(), format("Expected %s\nFound %s\nMissing lines in output %s", expected, actual, absentLines));
    }
}
