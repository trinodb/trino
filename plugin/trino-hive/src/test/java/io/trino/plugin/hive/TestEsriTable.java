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
import com.google.common.collect.ImmutableSet;
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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static org.assertj.core.api.Assertions.assertThat;

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
            throws IOException, ParseException
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
        List<MaterializedRow> rows = result.getMaterializedRows();

        // Verify we got the expected counties
        assertThat(rows).hasSize(3);

        // Verify we have all expected county names
        Set<String> countyNames = rows.stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toSet());
        assertThat(countyNames).isEqualTo(ImmutableSet.of("San Francisco", "Madera", "San Mateo"));

        // Load expected WKT values
        Map<String, String> expectedWkt = loadExpectedWkt("esri/counties_expected.txt");

        // Verify each county has a valid geometry by converting WKB to WKT and comparing
        WKBReader wkbReader = new WKBReader();
        WKTWriter wktWriter = new WKTWriter();
        for (MaterializedRow row : rows) {
            String name = (String) row.getField(0);
            byte[] bytes = (byte[]) row.getField(1);

            // Parse WKB and convert to WKT
            Geometry geometry = wkbReader.read(bytes);
            String actualWkt = wktWriter.write(geometry);

            // Verify WKT matches expected value
            assertThat(actualWkt)
                    .describedAs("WKT for county: %s", name)
                    .isEqualTo(expectedWkt.get(name));
        }

        assertQueryFails(
                "INSERT INTO counties VALUES ('esri fails writes', X'0102030405')",
                "Writing not supported for StorageFormat\\{serde=com\\.esri\\.hadoop\\.hive\\.serde\\.EsriJsonSerDe, inputFormat=com\\.esri\\.json\\.hadoop\\.EnclosedEsriJsonInputFormat, outputFormat=org\\.apache\\.hadoop\\.hive\\.ql\\.io\\.HiveIgnoreKeyTextOutputFormat\\}"
        );

        assertUpdate("DROP TABLE counties");
    }

    private static Map<String, String> loadExpectedWkt(String resourceName)
            throws IOException
    {
        Map<String, String> expected = new HashMap<>();
        String content = Resources.toString(Resources.getResource(resourceName), StandardCharsets.UTF_8);
        for (String line : content.split("\n")) {
            String[] parts = line.split("\t", 2);
            if (parts.length == 2) {
                expected.put(parts[0], parts[1]);
            }
        }
        return expected;
    }
}
