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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.URL;
import java.util.UUID;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;

public class TestHiveOrcWithShortZoneId
        extends AbstractTestQueryFramework
{
    private Location dataFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder()
                .addHiveProperty("hive.orc.read-legacy-short-zone-id", "true")
                .build();

        URL resourceLocation = Resources.getResource("with_short_zone_id/data/data.orc");

        TrinoFileSystem fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        dataFile = tempDir.appendPath("data.orc");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }

        return queryRunner;
    }

    @Test
    public void testSelectWithShortZoneId()
    {
        // When table is created using ORC file that contains short zone id in stripe footer
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_select_with_short_zone_id_",
                "(id INT, firstName VARCHAR, lastName VARCHAR) WITH (external_location = '%s')".formatted(dataFile.parentDirectory()))) {
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1, 'John', 'Doe')");
        }
    }

    @Test
    public void testSelectWithoutShortZoneId()
    {
        // When table is created by trino
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_select_without_short_zone_id_",
                "(id INT, firstName VARCHAR, lastName VARCHAR)",
                ImmutableList.of("2, 'Alice', 'Doe'"))) {
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (2, 'Alice', 'Doe')");
        }
    }
}
