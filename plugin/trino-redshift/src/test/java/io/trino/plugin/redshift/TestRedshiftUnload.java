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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import io.trino.operator.OperatorInfo;
import io.trino.operator.SplitOperatorInfo;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestRedshiftUnload
        extends AbstractTestQueries
{
    private static final String S3_UNLOAD_ROOT = requiredNonEmptySystemProperty("test.redshift.s3.unload.root");
    private static final String AWS_REGION = requiredNonEmptySystemProperty("test.redshift.aws.region");
    private static final String AWS_ACCESS_KEY = requiredNonEmptySystemProperty("test.redshift.aws.access-key");
    private static final String AWS_SECRET_KEY = requiredNonEmptySystemProperty("test.redshift.aws.secret-key");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .setConnectorProperties(
                        Map.of(
                                "redshift.unload-location", S3_UNLOAD_ROOT,
                                "redshift.unload-iam-role", IAM_ROLE,
                                "s3.region", AWS_REGION,
                                "s3.aws-access-key", AWS_ACCESS_KEY,
                                "s3.aws-secret-key", AWS_SECRET_KEY))
                .setInitialTables(List.of(NATION))
                .build();
    }

    @Test
    void testUnloadEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'redshift.unload_enabled'",
                "VALUES ('redshift.unload_enabled', 'true', 'true', 'boolean', 'Use UNLOAD for reading query results')");
    }

    @Test
    void testUnload()
    {
        assertQueryStats(
                getSession(),
                "SELECT nationkey, name FROM nation WHERE regionkey = 0",
                queryStats -> {
                    Map<String, String> splitInfo =
                            ((SplitOperatorInfo) queryStats.getOperatorSummaries()
                                    .stream()
                                    .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                                    .collect(onlyElement())
                                    .getInfo())
                                    .getSplitInfo();
                    Stream.of(splitInfo.get("path").split(", ")).forEach(path -> assertThat(path).matches("%s/.*/.*.parquet".formatted(S3_UNLOAD_ROOT)));
                },
                results -> assertThat(results.getRowCount()).isEqualTo(5));
    }

    @Test
    void testUnloadFallbackToJdbc()
    {
        // Fallback to JDBC as limit clause is not supported by UNLOAD
        assertQueryStats(
                getSession(),
                "SELECT nationkey, name FROM nation WHERE regionkey = 0 LIMIT 1",
                queryStats -> {
                    OperatorInfo operatorInfo = queryStats.getOperatorSummaries()
                            .stream()
                            .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                            .collect(onlyElement())
                            .getInfo();
                    assertThat(operatorInfo).isNull();
                },
                results -> assertThat(results.getRowCount()).isEqualTo(1));
    }

    @Test
    void testColumnName()
    {
        List<String> columnNames = ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                // .add("a\"quote") // TODO escape "(double quotes) in UNLOAD manifest(manifest contains invalid json `"name": "a"quote"`)
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash`")
                .add("a\\backslash`")
                .add("adigit0")
                .add("0startwithdigit")
                .add("カラム")
                .build();
        for (String columnName : columnNames) {
            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    private void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableNamePrefix = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA + "." + tableNamePrefix,
                "(%s varchar(50))".formatted(nameInSql),
                ImmutableList.of("'abc'"))) {
            assertQuery("SELECT " + nameInSql + " FROM " + table.getName(), "VALUES ('abc')");
        }
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

    private static SqlExecutor onRemoteDatabase()
    {
        return TestingRedshiftServer::executeInRedshift;
    }
}
