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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.nio.file.Files.createTempDirectory;

public class TestRegexTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of("hive.non-managed-table-writes-enabled", "true"))
                .build();
    }

    @Test
    public void testCreateExternalTableWithData()
            throws IOException
    {
        Path tempDir = createTempDirectory(null);
        Path tableLocation = tempDir.resolve("data");

        // REGEX format is read-only, so create data files using the text file format
        @Language("SQL") String createTableSql = """
                CREATE TABLE test_regex_data
                WITH (
                    format = 'textfile',
                    textfile_field_separator = 'x',
                    external_location = '%s')
                AS SELECT nationkey, name FROM tpch.tiny.nation
                """.formatted(tableLocation.toUri().toASCIIString());
        assertUpdate(createTableSql, 25);

        MaterializedResult expected = computeActual("SELECT nationkey, name FROM tpch.tiny.nation");
        MaterializedResult actual = computeActual("SELECT nationkey, name FROM test_regex_data");
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // REGEX table over the text file created data
        createTableSql = """
                CREATE TABLE test_regex (
                    nationkey BIGINT,
                    name VARCHAR)
                WITH (
                    format = 'regex',
                    regex = '(\\d+)x(.+)',
                    external_location = '%s')
                """.formatted(tableLocation.toUri().toASCIIString());
        assertUpdate(createTableSql);

        actual = computeActual("SELECT nationkey, name FROM test_regex");
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Verify REGEX read-only is enforced
        assertQueryFails("INSERT INTO test_regex VALUES (42, 'name')", "REGEX format is read-only");

        // case insensitive
        assertUpdate("DROP TABLE test_regex");
        createTableSql = """
                CREATE TABLE test_regex (
                    nationkey BIGINT,
                    name VARCHAR)
                WITH (
                    format = 'regex',
                    regex = '(\\d+)X(.+)',
                    regex_case_insensitive = true,
                    external_location = '%s')
                """.formatted(tableLocation.toUri().toASCIIString());
        assertUpdate(createTableSql);
        actual = computeActual("SELECT nationkey, name FROM test_regex");
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // case-sensitive with no-match
        assertUpdate("DROP TABLE test_regex");
        createTableSql = """
                CREATE TABLE test_regex (
                    nationkey BIGINT,
                    name VARCHAR)
                WITH (
                    format = 'regex',
                    regex = '(\\d+)X(.+)',
                    external_location = '%s')
                """.formatted(tableLocation.toUri().toASCIIString());
        assertUpdate(createTableSql);
        // when the pattern does not match all columns are null
        assertQueryReturnsEmptyResult("SELECT nationkey, name FROM test_regex WHERE nationkey IS NOT NULL AND name IS NOT NULL");

        assertUpdate("DROP TABLE test_regex");
        assertUpdate("DROP TABLE test_regex_data");
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testRegexPropertyIsRequired()
    {
        assertQueryFails("""
                CREATE TABLE test_regex_property_required (
                    nationkey BIGINT,
                    name VARCHAR)
                WITH (format = 'regex')
                """,
                "REGEX format requires the 'regex' table property");
    }

    @Test
    public void testInvalidRegexProperty()
    {
        assertQueryFails("""
                CREATE TABLE test_regex_property_required (
                    nationkey BIGINT,
                    name VARCHAR)
                WITH (
                    format = 'regex',
                    regex = '\\J')
                """,
                "Invalid REGEX pattern value: \\\\J");
    }
}
