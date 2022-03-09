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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logging;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.testing.CountingMockConnector;
import io.trino.testing.CountingMockConnector.MetadataCallsCount;
import io.trino.type.ColorType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertContains;
import static io.trino.jdbc.TestingJdbcUtils.array;
import static io.trino.jdbc.TestingJdbcUtils.assertResultSet;
import static io.trino.jdbc.TestingJdbcUtils.list;
import static io.trino.jdbc.TestingJdbcUtils.readRows;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTrinoDatabaseMetaData
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String COUNTING_CATALOG = "mock_catalog";

    private CountingMockConnector countingMockConnector;
    private TestingTrinoServer server;

    private Connection connection;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.create();

        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");

        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");

        server.installPlugin(new HivePlugin());
        server.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getBaseDataDir().resolve("hive").toAbsolutePath().toString())
                .put("hive.security", "sql-standard")
                .buildOrThrow());

        countingMockConnector = new CountingMockConnector();
        server.installPlugin(countingMockConnector.getPlugin());
        server.createCatalog(COUNTING_CATALOG, "mock", ImmutableMap.of());
        server.waitForNodeRefresh(Duration.ofSeconds(10));

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA blackhole.blackhole");
        }

        try (Connection connection = createConnection()) {
            connection.setCatalog("hive");
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET ROLE admin IN hive");
                statement.execute("CREATE SCHEMA default");
                statement.execute("CREATE TABLE default.test_table (a varchar)");
                statement.execute("CREATE VIEW default.test_view AS SELECT * FROM hive.default.test_table");
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        server.close();
        server = null;
        countingMockConnector = null;
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        connection.close();
    }

    @Test
    public void testGetClientInfoProperties()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getClientInfoProperties()) {
            assertResultSet(resultSet)
                    .hasColumnCount(4)
                    .hasColumn(1, "NAME", Types.VARCHAR)
                    .hasColumn(2, "MAX_LEN", Types.INTEGER)
                    .hasColumn(3, "DEFAULT_VALUE", Types.VARCHAR)
                    .hasColumn(4, "DESCRIPTION", Types.VARCHAR)
                    .hasRows((list(
                            list("ApplicationName", Integer.MAX_VALUE, null, null),
                            list("ClientInfo", Integer.MAX_VALUE, null, null),
                            list("ClientTags", Integer.MAX_VALUE, null, null),
                            list("TraceToken", Integer.MAX_VALUE, null, null))));
        }
    }

    @Test
    public void testPassEscapeInMetaDataQuery()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();

        Set<String> queries = captureQueries(() -> {
            String schemaPattern = "defau" + metaData.getSearchStringEscape() + "_t";
            try (ResultSet resultSet = metaData.getColumns("blackhole", schemaPattern, null, null)) {
                assertFalse(resultSet.next(), "There should be no results");
            }
            return null;
        });

        assertEquals(queries.size(), 1, "Expected exactly one query, got " + queries.size());
        String query = getOnlyElement(queries);

        assertContains(query, "_t' ESCAPE '", "Metadata query does not contain ESCAPE");
    }

    @Test
    public void testGetTypeInfo()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet typeInfo = metaData.getTypeInfo();
        while (typeInfo.next()) {
            int jdbcType = typeInfo.getInt("DATA_TYPE");
            switch (jdbcType) {
                case Types.BIGINT:
                    assertColumnSpec(typeInfo, Types.BIGINT, 19L, 10L, "bigint");
                    break;
                case Types.BOOLEAN:
                    assertColumnSpec(typeInfo, Types.BOOLEAN, null, null, "boolean");
                    break;
                case Types.INTEGER:
                    assertColumnSpec(typeInfo, Types.INTEGER, 10L, 10L, "integer");
                    break;
                case Types.DECIMAL:
                    assertColumnSpec(typeInfo, Types.DECIMAL, 38L, 10L, "decimal");
                    break;
                case Types.VARCHAR:
                    assertColumnSpec(typeInfo, Types.VARCHAR, null, null, "varchar");
                    break;
                case Types.TIMESTAMP:
                    assertColumnSpec(typeInfo, Types.TIMESTAMP, 23L, null, "timestamp");
                    break;
                case Types.DOUBLE:
                    assertColumnSpec(typeInfo, Types.DOUBLE, 53L, 2L, "double");
                    break;
            }
        }
    }

    @Test
    public void testGetUrl()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();
        assertEquals(metaData.getURL(), "jdbc:trino://" + server.getAddress());
    }

    @Test
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "Trino");
            assertEquals(metaData.getDatabaseProductVersion(), "testversion");
            assertEquals(metaData.getDatabaseMajorVersion(), 0);
            assertEquals(metaData.getDatabaseMinorVersion(), 0);
        }
    }

    @Test
    public void testGetUserName()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getUserName(), "admin");
        }
    }

    @Test
    public void testGetCatalogs()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                assertThat(readRows(rs))
                        .isEqualTo(list(list("blackhole"), list("hive"), list(COUNTING_CATALOG), list("system"), list(TEST_CATALOG)));

                ResultSetMetaData metadata = rs.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);
                assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);
            }
        }
    }

    @Test
    public void testGetSchemas()
            throws Exception
    {
        List<List<String>> hive = new ArrayList<>();
        hive.add(list("hive", "information_schema"));
        hive.add(list("hive", "default"));

        List<List<String>> countingCatalog = new ArrayList<>();
        countingCatalog.add(list(COUNTING_CATALOG, "information_schema"));
        countingCatalog.add(list(COUNTING_CATALOG, "test_schema1"));
        countingCatalog.add(list(COUNTING_CATALOG, "test_schema2"));

        List<List<String>> system = new ArrayList<>();
        system.add(list("system", "information_schema"));
        system.add(list("system", "jdbc"));
        system.add(list("system", "metadata"));
        system.add(list("system", "runtime"));

        List<List<String>> blackhole = new ArrayList<>();
        blackhole.add(list("blackhole", "information_schema"));
        blackhole.add(list("blackhole", "default"));
        blackhole.add(list("blackhole", "blackhole"));

        List<List<String>> test = new ArrayList<>();
        test.add(list(TEST_CATALOG, "information_schema"));
        for (String schema : TpchMetadata.SCHEMA_NAMES) {
            test.add(list(TEST_CATALOG, schema));
        }

        List<List<String>> all = new ArrayList<>();
        all.addAll(hive);
        all.addAll(countingCatalog);
        all.addAll(system);
        all.addAll(test);
        all.addAll(blackhole);

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                assertGetSchemasResult(rs, all);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, null)) {
                assertGetSchemasResult(rs, all);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, null)) {
                assertGetSchemasResult(rs, test);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("", null)) {
                // all schemas in Trino have a catalog name
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "information_schema")) {
                assertGetSchemasResult(rs, list(list(TEST_CATALOG, "information_schema")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "information_schema")) {
                assertGetSchemasResult(rs, list(
                        list(TEST_CATALOG, "information_schema"),
                        list(COUNTING_CATALOG, "information_schema"),
                        list("blackhole", "information_schema"),
                        list("hive", "information_schema"),
                        list("system", "information_schema")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sf_")) {
                assertGetSchemasResult(rs, list(list(TEST_CATALOG, "sf1")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sf%")) {
                List<List<String>> expected = test.stream()
                        .filter(item -> item.get(1).startsWith("sf"))
                        .collect(toList());
                assertGetSchemasResult(rs, expected);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", null)) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "unknown")) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "unknown")) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "unknown")) {
                assertGetSchemasResult(rs, list());
            }
        }
    }

    private static void assertGetSchemasResult(ResultSet rs, List<List<String>> expectedSchemas)
            throws SQLException
    {
        List<List<Object>> data = readRows(rs);

        assertThat(data).hasSize(expectedSchemas.size());
        for (List<Object> row : data) {
            assertThat(list((String) row.get(1), (String) row.get(0)))
                    .isIn(expectedSchemas);
        }

        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 2);

        assertEquals(metadata.getColumnLabel(1), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_CATALOG");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);
    }

    @Test
    public void testGetTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .contains(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, null, null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .contains(getTablesRow("information_schema", "schemata"));
            }
        }

        // no tables have an empty catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("", null, null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .contains(getTablesRow("information_schema", "schemata"));
            }
        }

        // no tables have an empty schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "", null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, "information_schema", null, null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .contains(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, "tables", null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, array("TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .contains(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "inf%", "tables", null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tab%", null)) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        // no matching catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("unknown", "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        // no matching schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "unknown", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        // no matching table
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "unknown", array("TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        // no matching type
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown", "TABLE"))) {
                assertTableMetadata(rs);
                assertThat(readRows(rs))
                        .contains(getTablesRow("information_schema", "tables"))
                        .doesNotContain(getTablesRow("information_schema", "schemata"));
            }
        }

        // empty type list
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array())) {
                assertTableMetadata(rs);
                assertThat(readRows(rs)).isEmpty();
            }
        }

        // view
        try (Connection connection = createConnection()) {
            for (boolean includeTable : new boolean[] {false, true}) {
                for (boolean includeView : new boolean[] {false, true}) {
                    List<String> types = new ArrayList<>();
                    List<List<Object>> expected = new ArrayList<>();
                    if (includeTable) {
                        types.add("TABLE");
                        expected.add(getTablesRow("hive", "default", "test_table", "TABLE"));
                    }
                    if (includeView) {
                        types.add("VIEW");
                        expected.add(getTablesRow("hive", "default", "test_view", "VIEW"));
                    }

                    try (ResultSet rs = connection.getMetaData().getTables("hive", "default", "test_%", types.toArray(new String[0]))) {
                        assertTableMetadata(rs);
                        Multiset<List<Object>> rows = ImmutableMultiset.copyOf(readRows(rs));
                        assertThat(rows).isEqualTo(ImmutableMultiset.copyOf(expected));
                    }
                }
            }
        }
    }

    private static List<Object> getTablesRow(String schema, String table)
    {
        return getTablesRow(TEST_CATALOG, schema, table, "TABLE");
    }

    private static List<Object> getTablesRow(String catalog, String schema, String table, String type)
    {
        return list(catalog, schema, table, type, null, null, null, null, null, null);
    }

    private static void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(6), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(8), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(9), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(10), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.VARCHAR);
    }

    @Test
    public void testGetTableTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                assertThat(readRows(tableTypes))
                        .isEqualTo(list(list("TABLE"), list("VIEW")));

                ResultSetMetaData metadata = tableTypes.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);

                assertEquals(metadata.getColumnLabel(1), "TABLE_TYPE");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);
            }
        }
    }

    @Test
    public void testGetColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "blackhole");
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertEquals(rs.getString("TABLE_NAME"), "tables");
                assertEquals(rs.getString("COLUMN_NAME"), "table_name");
                assertEquals(rs.getString("IS_NULLABLE"), "YES");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "hive");
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), COUNTING_CATALOG);
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "system");
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "system");
                assertEquals(rs.getString("TABLE_SCHEM"), "jdbc");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), TEST_CATALOG);
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertFalse(rs.next());
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, null, "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertThat(readRows(rs)).hasSize(1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertThat(readRows(rs)).hasSize(5);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertThat(readRows(rs)).hasSize(1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "inf%", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertThat(readRows(rs)).hasSize(1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tab%", "table_name")) {
                assertColumnMetadata(rs);
                assertThat(readRows(rs)).hasSize(2);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "%m%")) {
                assertColumnMetadata(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "table_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "table_name");
                assertFalse(rs.next());
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "tiny", "supplier", "suppkey")) {
                assertColumnMetadata(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString("IS_NULLABLE"), "NO");
            }
        }

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate(
                    "CREATE TABLE test_get_columns_table (" +
                            "c_boolean boolean, " +
                            "c_bigint bigint, " +
                            "c_integer integer, " +
                            "c_smallint smallint, " +
                            "c_tinyint tinyint, " +
                            "c_real real, " +
                            "c_double double, " +
                            "c_varchar_1234 varchar(1234), " +
                            "c_varchar varchar, " +
                            "c_char_345 char(345), " +
                            "c_varbinary varbinary, " +
                            "c_time time, " +
                            "c_time_0 time(0), " +
                            "c_time_3 time(3), " +
                            "c_time_6 time(6), " +
                            "c_time_9 time(9), " +
                            "c_time_12 time(12), " +
                            "c_time_with_time_zone time with time zone, " +
                            "c_time_with_time_zone_0 time(0) with time zone, " +
                            "c_time_with_time_zone_3 time(3) with time zone, " +
                            "c_time_with_time_zone_6 time(6) with time zone, " +
                            "c_time_with_time_zone_9 time(9) with time zone, " +
                            "c_time_with_time_zone_12 time(12) with time zone, " +
                            "c_timestamp timestamp, " +
                            "c_timestamp_0 timestamp(0), " +
                            "c_timestamp_3 timestamp(3), " +
                            "c_timestamp_6 timestamp(6), " +
                            "c_timestamp_9 timestamp(9), " +
                            "c_timestamp_12 timestamp(12), " +
                            "c_timestamp_with_time_zone timestamp with time zone, " +
                            "c_timestamp_with_time_zone_0 timestamp(0) with time zone, " +
                            "c_timestamp_with_time_zone_3 timestamp(3) with time zone, " +
                            "c_timestamp_with_time_zone_6 timestamp(6) with time zone, " +
                            "c_timestamp_with_time_zone_9 timestamp(9) with time zone, " +
                            "c_timestamp_with_time_zone_12 timestamp(12) with time zone, " +
                            "c_date date, " +
                            "c_decimal_8_2 decimal(8,2), " +
                            "c_decimal_38_0 decimal(38,0), " +
                            "c_array array<bigint>, " +
                            "c_color color" +
                            ")"), 0);

            try (ResultSet rs = connection.getMetaData().getColumns("blackhole", "blackhole", "test_get_columns_table", null)) {
                assertColumnMetadata(rs);
                assertColumnSpec(rs, Types.BOOLEAN, null, null, null, null, BooleanType.BOOLEAN);
                assertColumnSpec(rs, Types.BIGINT, 19L, 10L, null, null, BigintType.BIGINT);
                assertColumnSpec(rs, Types.INTEGER, 10L, 10L, null, null, IntegerType.INTEGER);
                assertColumnSpec(rs, Types.SMALLINT, 5L, 10L, null, null, SmallintType.SMALLINT);
                assertColumnSpec(rs, Types.TINYINT, 3L, 10L, null, null, TinyintType.TINYINT);
                assertColumnSpec(rs, Types.REAL, 24L, 2L, null, null, RealType.REAL);
                assertColumnSpec(rs, Types.DOUBLE, 53L, 2L, null, null, DoubleType.DOUBLE);
                assertColumnSpec(rs, Types.VARCHAR, 1234L, null, null, 1234L, createVarcharType(1234));
                assertColumnSpec(rs, Types.VARCHAR, (long) Integer.MAX_VALUE, null, null, (long) Integer.MAX_VALUE, createUnboundedVarcharType());
                assertColumnSpec(rs, Types.CHAR, 345L, null, null, 345L, createCharType(345));
                assertColumnSpec(rs, Types.VARBINARY, (long) Integer.MAX_VALUE, null, null, (long) Integer.MAX_VALUE, VarbinaryType.VARBINARY);
                assertColumnSpec(rs, Types.TIME, 12L, null, 3L, null, TimeType.TIME);
                assertColumnSpec(rs, Types.TIME, 8L, null, 0L, null, createTimeType(0));
                assertColumnSpec(rs, Types.TIME, 12L, null, 3L, null, createTimeType(3));
                assertColumnSpec(rs, Types.TIME, 15L, null, 6L, null, createTimeType(6));
                assertColumnSpec(rs, Types.TIME, 18L, null, 9L, null, createTimeType(9));
                assertColumnSpec(rs, Types.TIME, 21L, null, 12L, null, createTimeType(12));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 18L, null, 3L, null, TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 14L, null, 0L, null, createTimeWithTimeZoneType(0));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 18L, null, 3L, null, createTimeWithTimeZoneType(3));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 21L, null, 6L, null, createTimeWithTimeZoneType(6));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 24L, null, 9L, null, createTimeWithTimeZoneType(9));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 27L, null, 12L, null, createTimeWithTimeZoneType(12));
                assertColumnSpec(rs, Types.TIMESTAMP, 25L, null, 3L, null, TimestampType.TIMESTAMP);
                assertColumnSpec(rs, Types.TIMESTAMP, 21L, null, 0L, null, createTimestampType(0));
                assertColumnSpec(rs, Types.TIMESTAMP, 25L, null, 3L, null, createTimestampType(3));
                assertColumnSpec(rs, Types.TIMESTAMP, 28L, null, 6L, null, createTimestampType(6));
                assertColumnSpec(rs, Types.TIMESTAMP, 31L, null, 9L, null, createTimestampType(9));
                assertColumnSpec(rs, Types.TIMESTAMP, 34L, null, 12L, null, createTimestampType(12));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 59L, null, 3L, null, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 55L, null, 0L, null, createTimestampWithTimeZoneType(0));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 59L, null, 3L, null, createTimestampWithTimeZoneType(3));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 62L, null, 6L, null, createTimestampWithTimeZoneType(6));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 65L, null, 9L, null, createTimestampWithTimeZoneType(9));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 68L, null, 12L, null, createTimestampWithTimeZoneType(12));
                assertColumnSpec(rs, Types.DATE, 14L, null, null, null, DateType.DATE);
                assertColumnSpec(rs, Types.DECIMAL, 8L, 10L, 2L, null, createDecimalType(8, 2));
                assertColumnSpec(rs, Types.DECIMAL, 38L, 10L, 0L, null, createDecimalType(38, 0));
                assertColumnSpec(rs, Types.ARRAY, null, null, null, null, new ArrayType(BigintType.BIGINT));
                assertColumnSpec(rs, Types.JAVA_OBJECT, null, null, null, null, ColorType.COLOR);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertColumnSpec(ResultSet rs, int jdbcType, Long columnSize, Long numPrecRadix, Long decimalDigits, Long charOctetLength, Type type)
            throws SQLException
    {
        String message = " of " + type.getDisplayName() + ": ";
        assertTrue(rs.next());
        assertEquals(rs.getObject("TYPE_NAME"), type.getDisplayName(), "TYPE_NAME");
        assertEquals(rs.getObject("DATA_TYPE"), (long) jdbcType, "DATA_TYPE" + message);
        assertEquals(rs.getObject("COLUMN_SIZE"), columnSize, "COLUMN_SIZE" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), numPrecRadix, "NUM_PREC_RADIX" + message);
        assertEquals(rs.getObject("DECIMAL_DIGITS"), decimalDigits, "DECIMAL_DIGITS" + message);
        assertEquals(rs.getObject("CHAR_OCTET_LENGTH"), charOctetLength, "CHAR_OCTET_LENGTH" + message);
    }

    private static void assertColumnMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 24);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "COLUMN_NAME");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "DATA_TYPE");
        assertEquals(metadata.getColumnType(5), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(6), "TYPE_NAME");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "COLUMN_SIZE");
        assertEquals(metadata.getColumnType(7), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(8), "BUFFER_LENGTH");
        assertEquals(metadata.getColumnType(8), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(9), "DECIMAL_DIGITS");
        assertEquals(metadata.getColumnType(9), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(10), "NUM_PREC_RADIX");
        assertEquals(metadata.getColumnType(10), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(11), "NULLABLE");
        assertEquals(metadata.getColumnType(11), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(12), "REMARKS");
        assertEquals(metadata.getColumnType(12), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(13), "COLUMN_DEF");
        assertEquals(metadata.getColumnType(13), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(14), "SQL_DATA_TYPE");
        assertEquals(metadata.getColumnType(14), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(15), "SQL_DATETIME_SUB");
        assertEquals(metadata.getColumnType(15), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(16), "CHAR_OCTET_LENGTH");
        assertEquals(metadata.getColumnType(16), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(17), "ORDINAL_POSITION");
        assertEquals(metadata.getColumnType(17), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(18), "IS_NULLABLE");
        assertEquals(metadata.getColumnType(18), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(19), "SCOPE_CATALOG");
        assertEquals(metadata.getColumnType(19), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(20), "SCOPE_SCHEMA");
        assertEquals(metadata.getColumnType(20), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(21), "SCOPE_TABLE");
        assertEquals(metadata.getColumnType(21), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(22), "SOURCE_DATA_TYPE");
        assertEquals(metadata.getColumnType(22), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(23), "IS_AUTOINCREMENT");
        assertEquals(metadata.getColumnType(23), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(24), "IS_GENERATEDCOLUMN");
        assertEquals(metadata.getColumnType(24), Types.VARCHAR);
    }

    @Test
    public void testGetPseudoColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getPseudoColumns(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetProcedures()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedures(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetProcedureColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedureColumns(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetSuperTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTables(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetUdts()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getUDTs(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetAttributes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getAttributes(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetSuperTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTypes(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetSchemasMetadataCalls()
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(null, null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                new MetadataCallsCount()
                        .withListSchemasCount(1));

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(
                        list(COUNTING_CATALOG, "information_schema"),
                        list(COUNTING_CATALOG, "test_schema1"),
                        list(COUNTING_CATALOG, "test_schema2")),
                new MetadataCallsCount()
                        .withListSchemasCount(1));

        // Equality predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, "test\\_schema%"),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(
                        list(COUNTING_CATALOG, "test_schema1"),
                        list(COUNTING_CATALOG, "test_schema2")),
                new MetadataCallsCount()
                        .withListSchemasCount(1));

        // LIKE predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, "test_sch_ma1"),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(list(COUNTING_CATALOG, "test_schema1")),
                new MetadataCallsCount()
                        .withListSchemasCount(1));

        // Empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, ""),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1));

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas("wrong", null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(),
                new MetadataCallsCount());
    }

    @Test
    public void testGetTablesMetadataCalls()
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(null, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // Equality predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test\\_schema1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                countingMockConnector.getAllTables()
                        .filter(schemaTableName -> schemaTableName.getSchemaName().equals("test_schema1"))
                        .map(schemaTableName -> list(COUNTING_CATALOG, schemaTableName.getSchemaName(), schemaTableName.getTableName(), "TABLE"))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListTablesCount(1));

        // LIKE predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_sch_ma1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                countingMockConnector.getAllTables()
                        .filter(schemaTableName -> schemaTableName.getSchemaName().equals("test_schema1"))
                        .map(schemaTableName -> list(COUNTING_CATALOG, schemaTableName.getSchemaName(), schemaTableName.getTableName(), "TABLE"))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // Equality predicate on table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(
                        list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                        list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // LIKE predicate on table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "test_t_ble1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(
                        list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                        list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // Equality predicate on schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                new MetadataCallsCount()
                        .withGetTableHandleCount(1));

        // LIKE predicate on schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables("wrong", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                new MetadataCallsCount());

        // empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // empty table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));

        // no table types selected
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, null, new String[0]),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                new MetadataCallsCount());
    }

    @Test
    public void testGetColumnsMetadataCalls()
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2)
                        .withGetColumnsCount(3000));

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2)
                        .withGetColumnsCount(3000));

        // Equality predicate on catalog name and schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                new MetadataCallsCount()
                        .withListSchemasCount(0)
                        .withListTablesCount(1)
                        .withGetColumnsCount(1000));

        // Equality predicate on catalog name, schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.range(0, 100)
                        .mapToObj(i -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + i, "varchar"))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListTablesCount(1)
                        .withGetColumnsCount(1));

        // Equality predicate on catalog name, schema name, table name and column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", "column\\_17"),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                new MetadataCallsCount()
                        .withListTablesCount(1)
                        .withGetColumnsCount(1));

        // Equality predicate on catalog name, LIKE predicate on schema name, table name and column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17"),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                new MetadataCallsCount()
                        .withListSchemasCount(2)
                        .withListTablesCount(3)
                        .withGetColumnsCount(1));

        // LIKE predicate on schema name and table name, but no predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.range(0, 100)
                        .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + columnIndex, "varchar"))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListSchemasCount(2)
                        .withListTablesCount(3)
                        .withGetColumnsCount(1));

        // LIKE predicate on schema name, but no predicate on catalog name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test_schema1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.range(0, 1000).boxed()
                        .flatMap(tableIndex ->
                                IntStream.range(0, 100)
                                        .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table" + tableIndex, "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListSchemasCount(2)
                        .withListTablesCount(1)
                        .withGetColumnsCount(1000));

        // LIKE predicate on table name, but no predicate on catalog name and schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, null, "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.rangeClosed(1, 2).boxed()
                        .flatMap(schemaIndex ->
                                IntStream.range(0, 100)
                                        .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema" + schemaIndex, "test_table1", "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListSchemasCount(3)
                        .withListTablesCount(8)
                        .withGetTableHandleCount(2)
                        .withGetColumnsCount(2));

        // Equality predicate on schema name and table name, but no predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.range(0, 100)
                        .mapToObj(i -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + i, "varchar"))
                        .collect(toImmutableList()),
                new MetadataCallsCount()
                        .withListTablesCount(1)
                        .withGetColumnsCount(1));

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns("wrong", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount());

        // schema does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "wrong\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount()
                        .withListTablesCount(1));

        // schema does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "wrong_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(2)
                        .withListTablesCount(0)
                        .withGetColumnsCount(0));

        // empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(0)
                        .withGetColumnsCount(0));

        // empty table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, "", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(0)
                        .withGetColumnsCount(0));

        // empty column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, null, ""),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2)
                        .withGetColumnsCount(3000));
    }

    @Test
    public void testAssumeLiteralMetadataCalls()
            throws Exception
    {
        try (Connection connection = DriverManager.getConnection(
                format("jdbc:trino://%s?assumeLiteralNamesInMetadataCallsForNonConformingClients=true", server.getAddress()),
                "admin",
                null)) {
            // getTables's schema name pattern treated as literal
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema1", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    countingMockConnector.getAllTables()
                            .filter(schemaTableName -> schemaTableName.getSchemaName().equals("test_schema1"))
                            .map(schemaTableName -> list(COUNTING_CATALOG, schemaTableName.getSchemaName(), schemaTableName.getTableName(), "TABLE"))
                            .collect(toImmutableList()),
                    new MetadataCallsCount()
                            .withListSchemasCount(0)
                            .withListTablesCount(1));

            // getTables's schema and table name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                    new MetadataCallsCount()
                            .withGetTableHandleCount(1));

            // no matches in getTables call as table name pattern treated as literal
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema_", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(),
                    new MetadataCallsCount()
                            .withListTablesCount(1));

            // getColumns's schema and table name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    IntStream.range(0, 100)
                            .mapToObj(i -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + i, "varchar"))
                            .collect(toImmutableList()),
                    new MetadataCallsCount()
                            .withListTablesCount(1)
                            .withGetColumnsCount(1));

            // getColumns's schema, table and column name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17"),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                    new MetadataCallsCount()
                            .withListTablesCount(1)
                            .withGetColumnsCount(1));

            // no matches in getColumns call as table name pattern treated as literal
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table_", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(),
                    new MetadataCallsCount()
                            .withListTablesCount(1));
        }
    }

    @Test
    public void testEscapeIfNecessary()
    {
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, null), null);
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, "a"), "a");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, "abc_def"), "abc_def");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, "abc__de_f"), "abc__de_f");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, "abc%def"), "abc%def");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(false, "abc\\_def"), "abc\\_def");

        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, null), null);
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, "a"), "a");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, "abc_def"), "abc\\_def");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, "abc__de_f"), "abc\\_\\_de\\_f");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, "abc%def"), "abc\\%def");
        assertEquals(TrinoDatabaseMetaData.escapeIfNecessary(true, "abc\\_def"), "abc\\\\\\_def");
    }

    @Test
    public void testStatementsDoNotLeak()
            throws Exception
    {
        TrinoConnection connection = (TrinoConnection) this.connection;
        DatabaseMetaData metaData = connection.getMetaData();

        // consumed
        try (ResultSet resultSet = metaData.getCatalogs()) {
            assertThat(countRows(resultSet)).isEqualTo(5);
        }
        try (ResultSet resultSet = metaData.getSchemas(TEST_CATALOG, null)) {
            assertThat(countRows(resultSet)).isEqualTo(10);
        }
        try (ResultSet resultSet = metaData.getTables(TEST_CATALOG, "sf%", null, null)) {
            assertThat(countRows(resultSet)).isEqualTo(64);
        }

        // not consumed
        metaData.getCatalogs().close();
        metaData.getSchemas(TEST_CATALOG, null).close();
        metaData.getTables(TEST_CATALOG, "sf%", null, null).close();

        assertThat(connection.activeStatements()).as("activeStatements")
                .isEqualTo(0);
    }

    private static void assertColumnSpec(ResultSet rs, int dataType, Long precision, Long numPrecRadix, String typeName)
            throws SQLException
    {
        String message = " of " + typeName + ": ";
        assertEquals(rs.getObject("TYPE_NAME"), typeName, "TYPE_NAME" + message);
        assertEquals(rs.getObject("DATA_TYPE"), (long) dataType, "DATA_TYPE" + message);
        assertEquals(rs.getObject("PRECISION"), precision, "PRECISION" + message);
        assertEquals(rs.getObject("LITERAL_PREFIX"), null, "LITERAL_PREFIX" + message);
        assertEquals(rs.getObject("LITERAL_SUFFIX"), null, "LITERAL_SUFFIX" + message);
        assertEquals(rs.getObject("CREATE_PARAMS"), null, "CREATE_PARAMS" + message);
        assertEquals(rs.getObject("NULLABLE"), (long) DatabaseMetaData.typeNullable, "NULLABLE" + message);
        assertEquals(rs.getObject("CASE_SENSITIVE"), false, "CASE_SENSITIVE" + message);
        assertEquals(rs.getObject("SEARCHABLE"), (long) DatabaseMetaData.typeSearchable, "SEARCHABLE" + message);
        assertEquals(rs.getObject("UNSIGNED_ATTRIBUTE"), null, "UNSIGNED_ATTRIBUTE" + message);
        assertEquals(rs.getObject("FIXED_PREC_SCALE"), false, "FIXED_PREC_SCALE" + message);
        assertEquals(rs.getObject("AUTO_INCREMENT"), null, "AUTO_INCREMENT" + message);
        assertEquals(rs.getObject("LOCAL_TYPE_NAME"), null, "LOCAL_TYPE_NAME" + message);
        assertEquals(rs.getObject("MINIMUM_SCALE"), 0L, "MINIMUM_SCALE" + message);
        assertEquals(rs.getObject("MAXIMUM_SCALE"), 0L, "MAXIMUM_SCALE" + message);
        assertEquals(rs.getObject("SQL_DATA_TYPE"), null, "SQL_DATA_TYPE" + message);
        assertEquals(rs.getObject("SQL_DATETIME_SUB"), null, "SQL_DATETIME_SUB" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), numPrecRadix, "NUM_PREC_RADIX" + message);
    }

    private Set<String> captureQueries(Callable<?> action)
            throws Exception
    {
        Set<QueryId> queryIdsBefore = server.getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQueryId)
                .collect(toImmutableSet());

        action.call();

        return server.getQueryManager().getQueries().stream()
                .filter(queryInfo -> !queryIdsBefore.contains(queryInfo.getQueryId()))
                .map(BasicQueryInfo::getQuery)
                .collect(toImmutableSet());
    }

    private void assertMetadataCalls(Connection connection, MetaDataCallback<? extends Collection<List<Object>>> callback, MetadataCallsCount expectedMetadataCallsCount)
    {
        assertMetadataCalls(
                connection,
                callback,
                actual -> {},
                expectedMetadataCallsCount);
    }

    private void assertMetadataCalls(
            Connection connection,
            MetaDataCallback<? extends Collection<List<Object>>> callback,
            Collection<List<?>> expected,
            MetadataCallsCount expectedMetadataCallsCount)
    {
        assertMetadataCalls(
                connection,
                callback,
                actual -> assertThat(ImmutableMultiset.copyOf(requireNonNull(actual, "actual is null")))
                        .isEqualTo(ImmutableMultiset.copyOf(requireNonNull(expected, "expected is null"))),
                expectedMetadataCallsCount);
    }

    private void assertMetadataCalls(
            Connection connection,
            MetaDataCallback<? extends Collection<List<Object>>> callback,
            Consumer<Collection<List<Object>>> resultsVerification,
            MetadataCallsCount expectedMetadataCallsCount)
    {
        MetadataCallsCount actualMetadataCallsCount = countingMockConnector.runCounting(() -> {
            try {
                Collection<List<Object>> actual = callback.apply(connection.getMetaData());
                resultsVerification.accept(actual);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(actualMetadataCallsCount, expectedMetadataCallsCount);
    }

    private MetaDataCallback<List<List<Object>>> readMetaData(MetaDataCallback<ResultSet> query, List<String> columns)
    {
        return metaData -> {
            try (ResultSet resultSet = query.apply(metaData)) {
                return readRows(resultSet, columns);
            }
        };
    }

    private int countRows(ResultSet resultSet)
            throws Exception
    {
        int rows = 0;
        while (resultSet.next()) {
            rows++;
        }
        return rows;
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        return DriverManager.getConnection(url, "admin", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "admin", null);
    }

    private interface MetaDataCallback<T>
    {
        T apply(DatabaseMetaData metaData)
                throws SQLException;
    }
}
