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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestOraclePoolRemarksReportingConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return OracleQueryRunner.createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.remarks-reporting.enabled", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }

    @Test
    @Override
    public void testCommentColumn()
    {
        String tableName = "test_comment_column_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(a integer)");

        // comment set
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'new comment'");
        // with remarksReporting Oracle does not return comments set
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("COMMENT 'new comment'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "testCommentDataProvider")
    public void testCommentColumnSpecialCharacter(String comment)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS " + varcharLiteral(comment));
            assertEquals(getColumnComment(table.getName(), "a"), comment);
        }
    }

    @DataProvider
    public Object[][] testCommentDataProvider()
    {
        return new Object[][] {
                {"a;semicolon"},
                {"an@at"},
                {"a\"quote"},
                {"an'apostrophe"},
                {"a`backtick`"},
                {"a/slash`"},
                {"a\\backslash`"},
                {"a?question"},
                {"[square bracket]"},
        };
    }

    private static String varcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''") + "'";
    }

    // This method is copied from BaseConnectorTest because oracle.remarks-reporting.enabled config property needs to be enabled
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testCommentColumnName(String columnName)
    {
        if (!requiresDelimiting(columnName)) {
            testCommentColumnName(columnName, false);
        }
        testCommentColumnName(columnName, true);
    }

    private void testCommentColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column", "(" + nameInSql + " integer)")) {
            assertUpdate("COMMENT ON COLUMN " + table.getName() + "." + nameInSql + " IS 'test comment'");
            assertThat(getColumnComment(table.getName(), columnName.replace("'", "''").toLowerCase(ENGLISH))).isEqualTo("test comment");
        }
        catch (RuntimeException e) {
            if (getStackTraceAsString(e).contains("CREATE TABLE") &&
                    (columnName.equals("a\"quote") || columnName.equals("\"STATS_MIXED_QUOTED_UPPER\"") || columnName.equals("\"stats_mixed_quoted_lower\"") || columnName.equals("\"stats_mixed_QuoTeD_miXED\""))) {
                return;
            }
            throw e;
        }
    }

    @DataProvider
    public Object[][] testColumnNameDataProvider()
    {
        return new Object[][] {
                {"STATS_MIXED_UNQUOTED_UPPER"},
                {"stats_mixed_unquoted_lower"},
                {"stats_mixed_uNQuoTeD_miXED"},
                {"\"STATS_MIXED_QUOTED_UPPER\""},
                {"\"stats_mixed_quoted_lower\""},
                {"\"stats_mixed_QuoTeD_miXED\""},
                {"lowercase"},
                {"UPPERCASE"},
                {"MixedCase"},
                {"an_underscore"},
                {"a-hyphen-minus"}, // ASCII '-' is HYPHEN-MINUS in Unicode
                {"a space"},
                {"atrailingspace "},
                {" aleadingspace"},
                {"a.dot"},
                {"a,comma"},
                {"a:colon"},
                {"a;semicolon"},
                {"an@at"},
                {"a\"quote"},
                {"an'apostrophe"},
                {"a`backtick`"},
                {"a/slash`"},
                {"a\\backslash`"},
                {"adigit0"},
                {"0startwithdigit"}
        };
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    private static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    protected String getColumnComment(String tableName, String columnName)
    {
        MaterializedResult materializedResult = computeActual(format(
                "SELECT comment FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
        return (String) materializedResult.getOnlyValue();
    }
}
