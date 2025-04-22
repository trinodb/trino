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
import java.math.BigDecimal;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
        URL resourceLocation = Resources.getResource("grok/simple_log");
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));

        // Create a temporary directory for the table data
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("simple_log");

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

        String inputFormat = "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}";

        // GROK format is read-only, so create data files using the text file format
        @Language("SQL") String createTableSql =
                """
                CREATE TABLE test_grok_table (
                    client VARCHAR,
                    method VARCHAR,
                    request VARCHAR,
                    bytes BIGINT,
                    duration BIGINT)
                WITH (
                    format = 'grok',
                    grok_input_format = '%s',
                    external_location = '%s')
                """.formatted(inputFormat, dataFile.parentDirectory());

       assertUpdate(createTableSql);
       MaterializedResult result = computeActual("SELECT * FROM test_grok_table");
        List<MaterializedRow> expected = List.of(
                new MaterializedRow(Arrays.asList(
                        "55.3.244.1",
                        "GET",
                        "/index.html",
                        15824L,
                        10L
                )),
                new MaterializedRow(Arrays.asList(
                        "10.0.0.15",
                        "POST",
                        "/login.php",
                        2341L,
                        15L
                )),
                new MaterializedRow(Arrays.asList(
                        "144.76.92.155",
                        "GET",
                        "/downloads/file.zip",
                        234567L,
                        45L
                )),
                new MaterializedRow(Arrays.asList(
                        null,
                        null,
                        null,
                        null,
                        null
                )),
                new MaterializedRow(Arrays.asList(
                        "209.85.231.104",
                        "POST",
                        "/checkout",
                        12345L,
                        22L
                ))
        );
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expected);
        assertQueryFails("INSERT INTO test_grok_table VALUES ('a', 'b', 'c', 0, 0)", "GROK format is read-only");

        assertUpdate("DROP TABLE test_grok_table");
    }

    @Test
    public void testGrokSupportedDataTypes()
            throws IOException
    {
        URL resourceLocation = Resources.getResource("grok/supported_datatypes_log");
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));

        // Create a temporary directory for the table data
        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("supported_datatypes_log");

        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

        String inputFormat = "%{WORD:a} %{NUMBER:b} %{BASE10NUM:c} %{POSINT:d} %{NONNEGINT:e} %{NOTSPACE:f} %{BASE10NUM:g} %{NUMBER:h} %{NOTSPACE:i} %{TIMESTAMP_ISO8601:j} %{WORD:k} %{NOTSPACE:l}";

        // GROK format is read-only, so create data files using the text file format
        @Language("SQL") String createTableSql =
                """
                CREATE TABLE test_grok_supported_datatypes (
                    a BOOLEAN,
                    b BIGINT,
                    c INTEGER,
                    d SMALLINT,
                    e TINYINT,
                    f DECIMAL(10,2),
                    g REAL,
                    h DOUBLE,
                    i DATE,
                    j TIMESTAMP,
                    k VARCHAR,
                    l CHAR(4))
                WITH (
                    format = 'grok',
                    grok_input_format = '%s',
                    external_location = '%s')
                """.formatted(inputFormat, dataFile.parentDirectory());

        assertUpdate(createTableSql);
        MaterializedResult result = computeActual("SELECT * FROM test_grok_supported_datatypes");
        List<MaterializedRow> expected = List.of(
                new MaterializedRow(Arrays.asList(
                        true,
                        123456789L,
                        1000,
                        (short) 1,
                        (byte) 0,
                        new BigDecimal("999.99"),
                        3.14159f,
                        1.23,
                        LocalDate.of(2025, 1, 1),
                        LocalDateTime.of(LocalDate.of(2024, 1, 1), LocalTime.of(12, 0, 1)),
                        "Hello",
                        "abc " // test that null padding for char type works
                ))
        );
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expected);
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
