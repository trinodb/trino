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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.influxdb.InfluxDataTool.TEST_DATABASE_TEMPORARY;
import static io.trino.plugin.influxdb.InfluxDataTool.TEST_MEASUREMENT_W;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInfluxMetadataMutation
{
    private TestingInfluxServer server;
    private InfluxMetadata metadata;

    @BeforeClass
    public void setup()
    {
        server = new TestingInfluxServer();
        InfluxConfig config = new InfluxConfig();
        config.setEndpoint(server.getEndpoint());
        config.setUsername(TestingInfluxServer.USERNAME);
        config.setPassword(TestingInfluxServer.PASSWORD);
        metadata = new InfluxMetadata(new NativeInfluxClient(config));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
        server = null;
    }

    @Test(priority = 1)
    public void testCreateSchema()
    {
        metadata.createSchema(SESSION, TEST_DATABASE_TEMPORARY, ImmutableMap.of(),
                new TrinoPrincipal(PrincipalType.USER, "test"));
        assertThat(metadata.listSchemaNames(SESSION)).contains(TEST_DATABASE_TEMPORARY);
    }

    @Test(priority = 2)
    public void testDropTable()
    {
        // Create a table
        try (InfluxSession session = new InfluxSession(server.getEndpoint())) {
            InfluxDataTool tool = new InfluxDataTool(session);
            tool.setUpDataForTest(TEST_DATABASE_TEMPORARY, ImmutableList.of(TEST_MEASUREMENT_W));
        }
        assertThat(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_TEMPORARY)))
                .contains(new SchemaTableName(TEST_DATABASE_TEMPORARY, TEST_MEASUREMENT_W));

        // Drop the table
        InfluxTableHandle tableHandle = (InfluxTableHandle) metadata
                .getTableHandle(SESSION, new SchemaTableName(TEST_DATABASE_TEMPORARY, TEST_MEASUREMENT_W));
        metadata.dropTable(SESSION, tableHandle);

        assertThat(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_TEMPORARY)))
                .doesNotContain(new SchemaTableName(TEST_DATABASE_TEMPORARY, TEST_MEASUREMENT_W));
    }

    @Test(priority = 2)
    public void testDropUnknownTable()
    {
        InfluxTableHandle tableHandle = new InfluxTableHandle(TEST_DATABASE_TEMPORARY, "unknown");
        assertThatThrownBy(() -> metadata.dropTable(SESSION, tableHandle))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test(priority = 2)
    public void testDropUnknownSchema()
    {
        assertThatThrownBy(() -> metadata.dropSchema(SESSION, "unknown", false))
                .isInstanceOf(SchemaNotFoundException.class);
    }

    @Test(priority = 3)
    public void testDropSchemaCascade()
    {
        // Create a table
        try (InfluxSession session = new InfluxSession(server.getEndpoint())) {
            InfluxDataTool tool = new InfluxDataTool(session);
            tool.setUpDataForTest(TEST_DATABASE_TEMPORARY, ImmutableList.of(TEST_MEASUREMENT_W));
        }
        assertThat(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_TEMPORARY)))
                .contains(new SchemaTableName(TEST_DATABASE_TEMPORARY, TEST_MEASUREMENT_W));

        // Drop the schema with cascade
        metadata.dropSchema(SESSION, TEST_DATABASE_TEMPORARY, true);
        assertThat(metadata.listSchemaNames(SESSION)).doesNotContain(TEST_DATABASE_TEMPORARY);
    }

    @Test(priority = 4)
    public void testDropSchema()
    {
        // Create a schema and a table
        metadata.createSchema(SESSION, TEST_DATABASE_TEMPORARY, ImmutableMap.of(),
                new TrinoPrincipal(PrincipalType.USER, "test"));
        try (InfluxSession session = new InfluxSession(server.getEndpoint())) {
            InfluxDataTool tool = new InfluxDataTool(session);
            tool.setUpDataForTest(TEST_DATABASE_TEMPORARY, ImmutableList.of(TEST_MEASUREMENT_W));
        }
        assertThat(metadata.listTables(SESSION, Optional.of(TEST_DATABASE_TEMPORARY)))
                .contains(new SchemaTableName(TEST_DATABASE_TEMPORARY, TEST_MEASUREMENT_W));

        assertThatThrownBy(() -> metadata.dropSchema(SESSION, TEST_DATABASE_TEMPORARY, false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot drop non-empty schema '%s'".formatted(TEST_DATABASE_TEMPORARY));
    }
}
