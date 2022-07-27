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
package io.trino.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.BrokenConnector;
import io.trino.util.AutoCloseableCloser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static io.trino.jdbc.TestingJdbcUtils.readRows;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoDatabaseMetaDataWithBrokenCatalog
{
    private static final String BROKEN_CATALOG = "mock_catalog";

    private BrokenConnector brokenConnector;
    private TestingTrinoServer server;

    private Connection connection;

    @BeforeClass
    public void setupServer()
    {
        Logging.initialize();
        server = TestingTrinoServer.create();

        brokenConnector = new BrokenConnector();
        server.installPlugin(brokenConnector.getPlugin());
        server.createCatalog(BROKEN_CATALOG, "mock", ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            if (server != null) {
                closer.register(server);
                server = null;
            }
            if (connection != null) {
                closer.register(connection);
                connection = null;
            }
            brokenConnector = null;
        }
    }

    @Test
    public void testGetSchemasIgnoreBrokenCatalogsEnabled()
            throws Exception
    {
        try (Connection connection = createConnection(true)) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                assertThat(readRows(rs)).isNotEmpty();
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, null)) {
                assertThat(readRows(rs)).isNotEmpty();
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(BROKEN_CATALOG, "information_schema")) {
                assertThat(readRows(rs)).isNotEmpty();
            }
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void testGetSchemasIgnoreBrokenCatalogsDisabled()
            throws Exception
    {
        try (Connection connection = createConnection(false)) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                readRows(rs);
            }
        }
    }

    @Test
    public void testGetTablesIgnoreBrokenCatalogsEnabled()
            throws Exception
    {
        try (Connection connection = createConnection(true)) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertThat(readRows(rs)).isNotEmpty();
            }

            try (ResultSet rs = connection.getMetaData().getTables(BROKEN_CATALOG, null, null, null)) {
                assertThat(readRows(rs)).isNotEmpty();
            }
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void testGetTablesIgnoreBrokenCatalogsDisabled()
            throws Exception
    {
        try (Connection connection = createConnection(false)) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                readRows(rs);
            }
        }
    }

    @Test
    public void testGetColumnsIgnoreBrokenCatalogsEnabled()
            throws Exception
    {
        try (Connection connection = createConnection(true)) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "table_name")) {
                assertThat(readRows(rs)).hasSize(2);
            }
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void testGetColumnsIgnoreBrokenCatalogsDisabled()
            throws Exception
    {
        try (Connection connection = createConnection(false)) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "table_name")) {
                readRows(rs);
            }
        }
    }

    private Connection createConnection(boolean ignoreBrokenCatalogs)
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());

        Properties properties = new Properties();

        properties.setProperty("user", "some_user");
        properties.setProperty("sessionProperties", "ignore_broken_catalogs:" + ignoreBrokenCatalogs);

        return DriverManager.getConnection(url, properties);
    }
}
