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
package io.prestosql.plugin.hive;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.HiveQlTranslation.translateHiveQlToPrestoSql;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestHiveQlTranslation
{
    private final SqlParser parser = new SqlParser();

    @Test
    public void testIdentifiers()
    {
        assertTranslation(
                "SELECT * FROM nation",
                "SELECT * FROM nation");
        assertTranslation(
                "SELECT * FROM `nation`",
                "SELECT * FROM \"nation\"");
        assertTranslation(
                "SELECT `nation`.`nationkey` FROM `nation`",
                "SELECT \"nation\".\"nationkey\" FROM \"nation\"");
        assertTranslation(
                "SELECT * FROM `it's a table`",
                "SELECT * FROM \"it's a table\"");
    }

    @Test
    public void testNumberLiterals()
    {
        assertTranslation(
                "SELECT 1",
                "SELECT 1");
    }

    @Test
    public void testStringLiterals()
    {
        assertTranslation(
                "SELECT '`'",
                "SELECT '`'");
        assertTranslation(
                "SELECT 'it\\'s an \"apple\"'",
                "SELECT 'it''s an \"apple\"'");
        assertTranslation(
                "SELECT \"it's an \\\"apple\\\"\"",
                "SELECT 'it''s an \"apple\"'");
        assertTranslation(
                "SELECT \"`\"",
                "SELECT '`'");
        assertTranslation(
                "SELECT '\"'",
                "SELECT '\"'");
        assertTranslation(
                "SELECT \"'\"",
                "SELECT ''''");
        assertTranslation(
                "SELECT '\\'`'",
                "SELECT '''`'");
        assertTranslation(
                "SELECT '\\\\\\''",
                "SELECT '\\'''");
        assertTranslation(
                "SELECT \"\\'`\"",
                "SELECT '''`'");
    }

    @Test
    public void testStringLiteralsWithNewLine()
    {
        assertTranslation(
                "SELECT \"'\n'\"",
                "SELECT '''\n'''");
        assertTranslation(
                "SELECT '\\'\n`'",
                "SELECT '''\n`'");
        assertTranslation(
                "SELECT \"\\\n'`\"",
                "SELECT '\n''`'");
    }

    @Test
    public void testPredicates()
    {
        assertTranslation(
                "SELECT \"'\" = \"'\" OR false",
                "SELECT '''' = '''' OR false");
    }

    private void assertTranslation(String hiveSql, String expectedPrestoSql)
    {
        String actualPrestoSql = translateHiveQlToPrestoSql(hiveSql);
        assertEquals(actualPrestoSql, expectedPrestoSql);
        assertPrestoSqlIsParsable(expectedPrestoSql);
        assertPrestoSqlIsParsable(actualPrestoSql);
    }

    private void assertPrestoSqlIsParsable(String actualPrestoSql)
    {
        parser.createStatement(actualPrestoSql, new ParsingOptions());
    }
}
