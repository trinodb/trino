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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.prestosql.plugin.blackhole.BlackHolePlugin;
import io.prestosql.plugin.hive.HiveHadoop2Plugin;
import io.prestosql.plugin.tpch.TpchMetadata;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.type.ColorType;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertContains;
import static io.prestosql.jdbc.TestPrestoDriver.closeQuietly;
import static io.prestosql.jdbc.TestPrestoDriver.waitForNodeRefresh;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoDatabaseMetaData
{
    private static final String TEST_CATALOG = "test_catalog";

    private TestingPrestoServer server;

    private Connection connection;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();

        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");

        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");

        server.installPlugin(new HiveHadoop2Plugin());
        server.createCatalog("hive", "hive-hadoop2", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getBaseDataDir().resolve("hive").toAbsolutePath().toString())
                .put("hive.security", "sql-standard")
                .build());

        waitForNodeRefresh(server);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA blackhole.blackhole");

            statement.execute("USE hive.default");
            statement.execute("SET ROLE admin");
            statement.execute("CREATE SCHEMA default");
            statement.execute("CREATE TABLE default.test_table(a varchar)");
            statement.execute("CREATE VIEW default.test_view AS SELECT * FROM hive.default.test_table");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        server.close();
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
    {
        closeQuietly(connection);
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
        assertEquals(metaData.getURL(), "jdbc:presto://" + server.getAddress());
    }

    @Test
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "Presto");
            assertEquals(metaData.getDatabaseProductVersion(), "testversion");
            assertEquals(metaData.getDatabaseMajorVersion(), 0);
            assertEquals(metaData.getDatabaseMinorVersion(), 0);
        }
    }

    @Test
    public void testGetCatalogs()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                assertEquals(readRows(rs), list(list("blackhole"), list("hive"), list("system"), list(TEST_CATALOG)));

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
                // all schemas in presto have a catalog name
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "information_schema")) {
                assertGetSchemasResult(rs, list(list(TEST_CATALOG, "information_schema")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "information_schema")) {
                assertGetSchemasResult(rs, list(
                        list(TEST_CATALOG, "information_schema"),
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

        assertEquals(data.size(), expectedSchemas.size());
        for (List<Object> row : data) {
            assertTrue(expectedSchemas.contains(list((String) row.get(1), (String) row.get(0))));
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

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no tables have an empty catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("", null, null, null)) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no tables have an empty schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "", null, null)) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, array("TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "inf%", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tab%", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no matching catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("unknown", "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "unknown", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching table
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "unknown", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching type
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown", "TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // empty type list
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array())) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
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
                        Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                        assertThat(rows).containsExactlyInAnyOrder(expected.toArray(new List[0]));
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
                List<List<Object>> data = readRows(tableTypes);
                assertEquals(data, list(list("TABLE"), list("VIEW")));

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
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "hive");
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
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 4);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "inf%", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tab%", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 2);
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
                            "c_time_with_time_zone time with time zone, " +
                            "c_timestamp timestamp, " +
                            "c_timestamp_with_time_zone timestamp with time zone, " +
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
                assertColumnSpec(rs, Types.TIME, 8L, null, null, null, TimeType.TIME);
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 14L, null, null, null, TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
                assertColumnSpec(rs, Types.TIMESTAMP, 23L, null, null, null, TimestampType.TIMESTAMP);
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 29L, null, null, null, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
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

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "admin", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "admin", null);
    }

    private static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows.build();
    }

    @SafeVarargs
    private static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }

    @SafeVarargs
    private static <T> T[] array(T... elements)
    {
        return elements;
    }
}
