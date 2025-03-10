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
import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
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
            throws IOException
    {
        URL resourceLocation = Resources.getResource("grok/commonapachelog_test");
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));

        // Create a temporary directory for the table data
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("commonapachelog_test");

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

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
                """.formatted(inputFormat, dataFile.parentDirectory());

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
