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
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestEsriTable
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
        URL resourceLocation = Resources.getResource("esri/counties.json");
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));

        // Create a temporary directory for the table data
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("counties.json");

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

        List<MaterializedRow> expected = readExpectedResults("esri/counties_expected.txt");

        // ESRI format is read-only, so create data files using the text file format
        @Language("SQL") String createCountiesTableSql =
                """
                CREATE TABLE counties (
                    name varchar, boundaryshape varbinary)
                WITH (
                    format = 'esri',
                    external_location = '%s')
                """.formatted(dataFile.parentDirectory());
        assertUpdate(createCountiesTableSql);

        MaterializedResult result = computeActual("SELECT * FROM counties");

        assertEqualsIgnoreOrder(result.getMaterializedRows(), expected);

        assertQueryFails(
                "INSERT INTO counties VALUES ('esri fails writes', X'0102030405')",
                "Writing not supported for StorageFormat\\{serde=com\\.esri\\.hadoop\\.hive\\.serde\\.EsriJsonSerDe, inputFormat=com\\.esri\\.json\\.hadoop\\.EnclosedEsriJsonInputFormat, outputFormat=org\\.apache\\.hadoop\\.hive\\.ql\\.io\\.HiveIgnoreKeyTextOutputFormat\\}"
        );

        assertUpdate("DROP TABLE counties");
    }


    private static List<MaterializedRow> readExpectedResults(String resourcePath)
            throws IOException
    {
        URL resourceUrl = Resources.getResource(resourcePath);
        List<String> lines = Resources.readLines(resourceUrl, UTF_8);

        return lines.stream()
                .map(line -> {
                    String[] parts = line.split("\t");  // Assuming tab-separated values
                    return new MaterializedRow(Arrays.asList(
                            parts[0],  // name
                            hexToBytes(parts[1])  // hex string for boundaryshape
                    ));
                })
                .collect(toImmutableList());
    }

    private static byte[] hexToBytes(String hex) {
        // Remove 'X' prefix, spaces, and single quotes if present
        hex = hex.replaceAll("^X'|'$", "")  // Remove X' and trailing '
                .replaceAll("\\s+", "");      // Remove all whitespace

        return HexFormat.of().parseHex(hex);
    }
}
