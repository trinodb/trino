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
import io.trino.filesystem.Location;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;

public class TestGrokTable
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
            throws IOException, URISyntaxException
    {
        // Create a temporary directory for the table data
        Path tempDir = Files.createTempDirectory("grok_test_");

        // Copy the test resource to the temporary directory
        Path sourcePath = Paths.get(getClass().getClassLoader()
                .getResource("grok/commonapachelog_test").toURI());
        Path targetPath = tempDir.resolve("commonapachelog_test");
        Files.copy(sourcePath, targetPath);

        String inputFormat = "%{COMMONAPACHELOG:access_log}";

        // GROK format is read-only, so create data files using the text file format
        @Language("SQL") String createTableSql =
                """
                CREATE TABLE test_grok_table (
                    access_log VARCHAR)
                WITH (
                    format = 'grok',
                    grok_input_format = '%s',
                    external_location = '%s')
                """.formatted(inputFormat, Location.of(tempDir.toString()));

       assertUpdate(createTableSql);
       MaterializedResult result = computeActual("SELECT * FROM test_grok_table");
        List<MaterializedRow> expected = List.of(
                new MaterializedRow(Arrays.asList(
                        "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"
                ))
        );
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expected);
        assertQueryFails("INSERT INTO test_grok_table VALUES ('grok fails writes')", "GROK format is read-only");

        assertUpdate("DROP TABLE test_grok_table");
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testGrokInputFormatPropertyIsRequired()
    {
        assertQueryFails(
                """
                CREATE TABLE test_regex_property_required (
                    name VARCHAR)
                WITH (format = 'grok')
                """,
                "GROK format requires the 'grok_input_format' table property");
    }
}
