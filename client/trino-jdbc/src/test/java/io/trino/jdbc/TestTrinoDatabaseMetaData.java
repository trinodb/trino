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
import com.google.common.collect.ImmutableSet;
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
import io.trino.type.ColorType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
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
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestTrinoDatabaseMetaData
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String COUNTING_CATALOG = "mock_catalog";

    private CountingMockConnector countingMockConnector;
    private TestingTrinoServer server;

    @BeforeAll
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
                .put("bootstrap.quiet", "true")
                .put("fs.hadoop.enabled", "true")
                .buildOrThrow());

        countingMockConnector = new CountingMockConnector();
        server.installPlugin(countingMockConnector.getPlugin());
        server.createCatalog(COUNTING_CATALOG, "mock", ImmutableMap.of());

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA blackhole.blackhole");
            statement.executeUpdate("CREATE SCHEMA blackhole.test_schema1");
            statement.executeUpdate("CREATE TABLE blackhole.test_schema1.test_table1 (column_0 varchar, column_1 varchar)");
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

    @AfterAll
    public void tearDownServer()
            throws Exception
    {
        server.close();
        server = null;
        countingMockConnector.close();
        countingMockConnector = null;
    }

    @Test
    public void testGetClientInfoProperties()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet resultSet = metaData.getClientInfoProperties()) {
                assertResultSet(resultSet)
                        .hasColumnCount(4)
                        .hasColumn(1, "NAME", Types.VARCHAR)
                        .hasColumn(2, "MAX_LEN", Types.INTEGER)
                        .hasColumn(3, "DEFAULT_VALUE", Types.VARCHAR)
                        .hasColumn(4, "DESCRIPTION", Types.VARCHAR)
                        .hasRows(list(
                                list("ApplicationName", Integer.MAX_VALUE, null, null),
                                list("ClientInfo", Integer.MAX_VALUE, null, null),
                                list("ClientTags", Integer.MAX_VALUE, null, null),
                                list("TraceToken", Integer.MAX_VALUE, null, null)));
            }
        }
    }

    @Test
    public void testPassEscapeInMetaDataQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            Set<String> queries = captureQueries(() -> {
                String schemaPattern = "defau" + metaData.getSearchStringEscape() + "_t";
                try (ResultSet resultSet = metaData.getColumns("blackhole", schemaPattern, null, null)) {
                    assertThat(resultSet.next())
                            .describedAs("There should be no results")
                            .isFalse();
                }
                return null;
            });

            assertThat(queries.size())
                    .describedAs("Expected exactly one query, got " + queries.size())
                    .isEqualTo(1);
            String query = getOnlyElement(queries);

            assertThat(query).as("Metadata query does not contain ESCAPE").contains("_t' ESCAPE '");
        }
    }

    @Test
    public void testGetTypeInfo()
            throws Exception
    {
        try (Connection connection = createConnection()) {
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
    }

    @Test
    public void testGetUrl()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getURL()).isEqualTo("jdbc:trino://" + server.getAddress());
        }
    }

    @Test
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getDatabaseProductName()).isEqualTo("Trino");
            assertThat(metaData.getDatabaseProductVersion()).isEqualTo("testversion");
            assertThat(metaData.getDatabaseMajorVersion()).isEqualTo(0);
            assertThat(metaData.getDatabaseMinorVersion()).isEqualTo(0);
        }
    }

    @Test
    public void testGetUserName()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getUserName()).isEqualTo("admin");
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
                assertThat(metadata.getColumnCount()).isEqualTo(1);
                assertThat(metadata.getColumnLabel(1)).isEqualTo("TABLE_CAT");
                assertThat(metadata.getColumnType(1)).isEqualTo(Types.VARCHAR);
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
        countingCatalog.add(list(COUNTING_CATALOG, "test_schema3_empty"));
        countingCatalog.add(list(COUNTING_CATALOG, "test_schema4_empty"));

        List<List<String>> system = new ArrayList<>();
        system.add(list("system", "information_schema"));
        system.add(list("system", "jdbc"));
        system.add(list("system", "metadata"));
        system.add(list("system", "runtime"));

        List<List<String>> blackhole = new ArrayList<>();
        blackhole.add(list("blackhole", "information_schema"));
        blackhole.add(list("blackhole", "default"));
        blackhole.add(list("blackhole", "blackhole"));
        blackhole.add(list("blackhole", "test_schema1"));

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

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "")) {
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
        assertThat(metadata.getColumnCount()).isEqualTo(2);

        assertThat(metadata.getColumnLabel(1)).isEqualTo("TABLE_SCHEM");
        assertThat(metadata.getColumnType(1)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(2)).isEqualTo("TABLE_CATALOG");
        assertThat(metadata.getColumnType(2)).isEqualTo(Types.VARCHAR);
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
        assertThat(metadata.getColumnCount()).isEqualTo(10);

        assertThat(metadata.getColumnLabel(1)).isEqualTo("TABLE_CAT");
        assertThat(metadata.getColumnType(1)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(2)).isEqualTo("TABLE_SCHEM");
        assertThat(metadata.getColumnType(2)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(3)).isEqualTo("TABLE_NAME");
        assertThat(metadata.getColumnType(3)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(4)).isEqualTo("TABLE_TYPE");
        assertThat(metadata.getColumnType(4)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(5)).isEqualTo("REMARKS");
        assertThat(metadata.getColumnType(5)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(6)).isEqualTo("TYPE_CAT");
        assertThat(metadata.getColumnType(6)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(7)).isEqualTo("TYPE_SCHEM");
        assertThat(metadata.getColumnType(7)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(8)).isEqualTo("TYPE_NAME");
        assertThat(metadata.getColumnType(8)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(9)).isEqualTo("SELF_REFERENCING_COL_NAME");
        assertThat(metadata.getColumnType(9)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(10)).isEqualTo("REF_GENERATION");
        assertThat(metadata.getColumnType(10)).isEqualTo(Types.VARCHAR);
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
                assertThat(metadata.getColumnCount()).isEqualTo(1);

                assertThat(metadata.getColumnLabel(1)).isEqualTo("TABLE_TYPE");
                assertThat(metadata.getColumnType(1)).isEqualTo(Types.VARCHAR);
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
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo("blackhole");
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("information_schema");
                assertThat(rs.getString("TABLE_NAME")).isEqualTo("tables");
                assertThat(rs.getString("COLUMN_NAME")).isEqualTo("table_name");
                assertThat(rs.getLong("NULLABLE")).isEqualTo(DatabaseMetaData.columnNullable);
                assertThat(rs.getString("IS_NULLABLE")).isEqualTo("YES");
                assertThat(rs.getInt("DATA_TYPE")).isEqualTo(Types.VARCHAR);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo("hive");
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("information_schema");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo(COUNTING_CATALOG);
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("information_schema");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo("system");
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("information_schema");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo("system");
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("jdbc");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("TABLE_CAT")).isEqualTo(TEST_CATALOG);
                assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("information_schema");
                assertThat(rs.next()).isFalse();
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
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME")).isEqualTo("table_schema");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME")).isEqualTo("table_name");
                assertThat(rs.next()).isFalse();
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "tiny", "supplier", "suppkey")) {
                assertColumnMetadata(rs);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("NULLABLE")).isEqualTo(DatabaseMetaData.columnNoNulls);
                assertThat(rs.getString("IS_NULLABLE")).isEqualTo("NO");
            }
        }

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate(
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
                            ")")).isEqualTo(0);

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
                assertColumnSpec(rs, Types.TIME, 12L, null, 3L, null, TimeType.TIME_MILLIS);
                assertColumnSpec(rs, Types.TIME, 8L, null, 0L, null, createTimeType(0));
                assertColumnSpec(rs, Types.TIME, 12L, null, 3L, null, createTimeType(3));
                assertColumnSpec(rs, Types.TIME, 15L, null, 6L, null, createTimeType(6));
                assertColumnSpec(rs, Types.TIME, 18L, null, 9L, null, createTimeType(9));
                assertColumnSpec(rs, Types.TIME, 21L, null, 12L, null, createTimeType(12));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 18L, null, 3L, null, TimeWithTimeZoneType.TIME_TZ_MILLIS);
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 14L, null, 0L, null, createTimeWithTimeZoneType(0));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 18L, null, 3L, null, createTimeWithTimeZoneType(3));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 21L, null, 6L, null, createTimeWithTimeZoneType(6));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 24L, null, 9L, null, createTimeWithTimeZoneType(9));
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 27L, null, 12L, null, createTimeWithTimeZoneType(12));
                assertColumnSpec(rs, Types.TIMESTAMP, 25L, null, 3L, null, TimestampType.TIMESTAMP_MILLIS);
                assertColumnSpec(rs, Types.TIMESTAMP, 21L, null, 0L, null, createTimestampType(0));
                assertColumnSpec(rs, Types.TIMESTAMP, 25L, null, 3L, null, createTimestampType(3));
                assertColumnSpec(rs, Types.TIMESTAMP, 28L, null, 6L, null, createTimestampType(6));
                assertColumnSpec(rs, Types.TIMESTAMP, 31L, null, 9L, null, createTimestampType(9));
                assertColumnSpec(rs, Types.TIMESTAMP, 34L, null, 12L, null, createTimestampType(12));
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 59L, null, 3L, null, TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
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
                assertThat(rs.next()).isFalse();
            }
        }
    }

    private static void assertColumnSpec(ResultSet rs, int jdbcType, Long columnSize, Long numPrecRadix, Long decimalDigits, Long charOctetLength, Type type)
            throws SQLException
    {
        String message = " of " + type.getDisplayName() + ": ";
        assertThat(rs.next()).isTrue();
        assertThat(rs.getObject("TYPE_NAME"))
                .describedAs("TYPE_NAME")
                .isEqualTo(type.getDisplayName());
        assertThat(rs.getObject("DATA_TYPE"))
                .describedAs("DATA_TYPE" + message)
                .isEqualTo((long) jdbcType);
        assertThat(rs.getObject("COLUMN_SIZE"))
                .describedAs("COLUMN_SIZE" + message)
                .isEqualTo(columnSize);
        assertThat(rs.getObject("NUM_PREC_RADIX"))
                .describedAs("NUM_PREC_RADIX" + message)
                .isEqualTo(numPrecRadix);
        assertThat(rs.getObject("DECIMAL_DIGITS"))
                .describedAs("DECIMAL_DIGITS" + message)
                .isEqualTo(decimalDigits);
        assertThat(rs.getObject("CHAR_OCTET_LENGTH"))
                .describedAs("CHAR_OCTET_LENGTH" + message)
                .isEqualTo(charOctetLength);
    }

    private static void assertColumnMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertThat(metadata.getColumnCount()).isEqualTo(24);

        assertThat(metadata.getColumnLabel(1)).isEqualTo("TABLE_CAT");
        assertThat(metadata.getColumnType(1)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(2)).isEqualTo("TABLE_SCHEM");
        assertThat(metadata.getColumnType(2)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(3)).isEqualTo("TABLE_NAME");
        assertThat(metadata.getColumnType(3)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(4)).isEqualTo("COLUMN_NAME");
        assertThat(metadata.getColumnType(4)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(5)).isEqualTo("DATA_TYPE");
        assertThat(metadata.getColumnType(5)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(6)).isEqualTo("TYPE_NAME");
        assertThat(metadata.getColumnType(6)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(7)).isEqualTo("COLUMN_SIZE");
        assertThat(metadata.getColumnType(7)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(8)).isEqualTo("BUFFER_LENGTH");
        assertThat(metadata.getColumnType(8)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(9)).isEqualTo("DECIMAL_DIGITS");
        assertThat(metadata.getColumnType(9)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(10)).isEqualTo("NUM_PREC_RADIX");
        assertThat(metadata.getColumnType(10)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(11)).isEqualTo("NULLABLE");
        assertThat(metadata.getColumnType(11)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(12)).isEqualTo("REMARKS");
        assertThat(metadata.getColumnType(12)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(13)).isEqualTo("COLUMN_DEF");
        assertThat(metadata.getColumnType(13)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(14)).isEqualTo("SQL_DATA_TYPE");
        assertThat(metadata.getColumnType(14)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(15)).isEqualTo("SQL_DATETIME_SUB");
        assertThat(metadata.getColumnType(15)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(16)).isEqualTo("CHAR_OCTET_LENGTH");
        assertThat(metadata.getColumnType(16)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(17)).isEqualTo("ORDINAL_POSITION");
        assertThat(metadata.getColumnType(17)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(18)).isEqualTo("IS_NULLABLE");
        assertThat(metadata.getColumnType(18)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(19)).isEqualTo("SCOPE_CATALOG");
        assertThat(metadata.getColumnType(19)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(20)).isEqualTo("SCOPE_SCHEMA");
        assertThat(metadata.getColumnType(20)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(21)).isEqualTo("SCOPE_TABLE");
        assertThat(metadata.getColumnType(21)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(22)).isEqualTo("SOURCE_DATA_TYPE");
        assertThat(metadata.getColumnType(22)).isEqualTo(Types.BIGINT);

        assertThat(metadata.getColumnLabel(23)).isEqualTo("IS_AUTOINCREMENT");
        assertThat(metadata.getColumnType(23)).isEqualTo(Types.VARCHAR);

        assertThat(metadata.getColumnLabel(24)).isEqualTo("IS_GENERATEDCOLUMN");
        assertThat(metadata.getColumnType(24)).isEqualTo(Types.VARCHAR);
    }

    @Test
    public void testGetPseudoColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getPseudoColumns(null, null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetProcedures()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedures(null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetProcedureColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedureColumns(null, null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetSuperTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTables(null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetUdts()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getUDTs(null, null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetAttributes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getAttributes(null, null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetSuperTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTypes(null, null, null)) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void testGetSchemasMetadataCalls()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            testGetSchemasMetadataCalls(connection);
        }
    }

    @Test
    public void testGetSchemasMetadataCallsWithNullCatalogMeansCurrent()
            throws Exception
    {
        try (Connection connection = createConnectionWithNullCatalogMeansCurrent()) {
            verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

            testGetSchemasMetadataCalls(connection);

            // No filter without connection catalog - lists all schemas across all catalogs
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getSchemas(null, null),
                            list("TABLE_CATALOG", "TABLE_SCHEM")),
                    list(
                            list("blackhole", "information_schema"),
                            list("blackhole", "default"),
                            list("blackhole", "blackhole"),
                            list("blackhole", "test_schema1"),
                            list("hive", "information_schema"),
                            list("hive", "default"),
                            list(COUNTING_CATALOG, "information_schema"),
                            list(COUNTING_CATALOG, "test_schema1"),
                            list(COUNTING_CATALOG, "test_schema2"),
                            list(COUNTING_CATALOG, "test_schema3_empty"),
                            list(COUNTING_CATALOG, "test_schema4_empty"),
                            list("system", "information_schema"),
                            list("system", "jdbc"),
                            list("system", "metadata"),
                            list("system", "runtime"),
                            list(TEST_CATALOG, "information_schema"),
                            list(TEST_CATALOG, "sf1"),
                            list(TEST_CATALOG, "sf100"),
                            list(TEST_CATALOG, "sf1000"),
                            list(TEST_CATALOG, "sf10000"),
                            list(TEST_CATALOG, "sf100000"),
                            list(TEST_CATALOG, "sf300"),
                            list(TEST_CATALOG, "sf3000"),
                            list(TEST_CATALOG, "sf30000"),
                            list(TEST_CATALOG, "tiny")),
                    ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

            // set a different catalog to check if current catalog is used
            connection.setCatalog("system");
            // No filter with connection catalog - lists schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getSchemas(null, null),
                            list("TABLE_CATALOG", "TABLE_SCHEM")),
                    list(
                            list("system", "information_schema"),
                            list("system", "jdbc"),
                            list("system", "metadata"),
                            list("system", "runtime")),
                    ImmutableMultiset.of());

            // change the catalog back using a statement on the connection
            connection.createStatement().execute(String.format("USE %s.%s", COUNTING_CATALOG, "test_schema1"));
            // No filter with connection catalog - lists schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getSchemas(null, null),
                            list("TABLE_CATALOG", "TABLE_SCHEM")),
                    list(
                            list(COUNTING_CATALOG, "information_schema"),
                            list(COUNTING_CATALOG, "test_schema1"),
                            list(COUNTING_CATALOG, "test_schema2"),
                            list(COUNTING_CATALOG, "test_schema3_empty"),
                            list(COUNTING_CATALOG, "test_schema4_empty")),
                    ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

            // Equality predicate on schema name - lists matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getSchemas(null, "test\\_schema%"),
                            list("TABLE_CATALOG", "TABLE_SCHEM")),
                    list(
                            list(COUNTING_CATALOG, "test_schema1"),
                            list(COUNTING_CATALOG, "test_schema2"),
                            list(COUNTING_CATALOG, "test_schema3_empty"),
                            list(COUNTING_CATALOG, "test_schema4_empty")),
                    ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

            // LIKE predicate on schema name - lists matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getSchemas(null, "test_sch_ma1"),
                            list("TABLE_CATALOG", "TABLE_SCHEM")),
                    list(list(COUNTING_CATALOG, "test_schema1")),
                    ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));
        }
    }

    private void testGetSchemasMetadataCalls(Connection connection)
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(null, null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(
                        list(COUNTING_CATALOG, "information_schema"),
                        list(COUNTING_CATALOG, "test_schema1"),
                        list(COUNTING_CATALOG, "test_schema2"),
                        list(COUNTING_CATALOG, "test_schema3_empty"),
                        list(COUNTING_CATALOG, "test_schema4_empty")),
                ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

        // Equality predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, "test\\_schema%"),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(
                        list(COUNTING_CATALOG, "test_schema1"),
                        list(COUNTING_CATALOG, "test_schema2"),
                        list(COUNTING_CATALOG, "test_schema3_empty"),
                        list(COUNTING_CATALOG, "test_schema4_empty")),
                ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

        // LIKE predicate on schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, "test_sch_ma1"),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(list(COUNTING_CATALOG, "test_schema1")),
                ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

        // Empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas(COUNTING_CATALOG, ""),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(),
                ImmutableMultiset.of());

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas("wrong", null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(),
                ImmutableMultiset.of());

        // empty catalog name (means null filter)
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getSchemas("", null),
                        list("TABLE_CATALOG", "TABLE_SCHEM")),
                list(),
                ImmutableMultiset.of());
    }

    @Test
    public void testGetTablesMetadataCalls()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            testGetTablesMetadataCalls(connection);
        }
    }

    @Test
    public void testGetTablesMetadataCallsWithNullCatalogMeansCurrent()
            throws Exception
    {
        try (Connection connection = createConnectionWithNullCatalogMeansCurrent()) {
            verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

            testGetTablesMetadataCalls(connection);

            // No filter without connection catalog - lists all tables across all catalogs
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of("blackhole", "hive", COUNTING_CATALOG, "system", TEST_CATALOG)),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());

            // set a different catalog to check if current catalog is used
            connection.setCatalog("system");
            // No filter with connection catalog - lists tables in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of("system")),
                    ImmutableMultiset.of());

            // change the catalog back using a statement on the connection
            connection.createStatement().execute(String.format("USE %s.%s", COUNTING_CATALOG, "test_schema1"));
            // No filter with connection catalog - lists tables in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of(COUNTING_CATALOG)),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());

            // Equality predicate on schema name - lists tables from matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, "test\\_schema1", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    countingMockConnector.getAllTables()
                            .filter(schemaTableName -> schemaTableName.getSchemaName().equals("test_schema1"))
                            .map(schemaTableName -> list(COUNTING_CATALOG, schemaTableName.getSchemaName(), schemaTableName.getTableName(), "TABLE"))
                            .collect(toImmutableList()),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                            .build());

            // LIKE predicate on schema name - lists tables from matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, "test_sch_ma1", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    countingMockConnector.getAllTables()
                            .filter(schemaTableName -> schemaTableName.getSchemaName().equals("test_schema1"))
                            .map(schemaTableName -> list(COUNTING_CATALOG, schemaTableName.getSchemaName(), schemaTableName.getTableName(), "TABLE"))
                            .collect(toImmutableList()),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());

            // Equality predicate on table name - lists matching tables in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, null, "test\\_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(
                            list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                            list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());

            // LIKE predicate on table name - lists matching tables in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, null, "test_t_ble1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(
                            list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                            list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());

            // Equality predicate on schema name and table name - lists matching tables from matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, "test\\_schema1", "test\\_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.isView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .build());

            // LIKE predicate on schema name and table name - lists matching tables from matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(null, "test_schema1", "test_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes")
                            .build());
        }
    }

    private void testGetTablesMetadataCalls(Connection connection)
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(null, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

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
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                        .build());

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
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

        // Equality predicate on table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(
                        list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                        list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

        // LIKE predicate on table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "test_t_ble1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(
                        list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE"),
                        list(COUNTING_CATALOG, "test_schema2", "test_table1", "TABLE")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

        // Equality predicate on schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.isView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .build());

        // LIKE predicate on schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables("wrong", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                ImmutableMultiset.of());

        // empty catalog name (means null filter)
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables("", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                ImmutableMultiset.of());

        // empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                ImmutableMultiset.of());

        // empty table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, "", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                ImmutableMultiset.of());

        // no table types selected
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, null, null, new String[0]),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                list(),
                ImmutableMultiset.of());
    }

    @Test
    public void testGetColumnsMetadataCalls()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            testGetColumnsMetadataCalls(connection);
        }
    }

    @Test
    public void testGetColumnsMetadataCallsWithNullCatalogMeansCurrent()
            throws Exception
    {
        try (Connection connection = createConnectionWithNullCatalogMeansCurrent()) {
            verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

            testGetColumnsMetadataCalls(connection);

            // No filter without connection catalog - lists all columns across all catalogs
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of("blackhole", "hive", COUNTING_CATALOG, "system", TEST_CATALOG)),
                    ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns"));

            // set a different catalog to check if current catalog is used
            connection.setCatalog("system");
            // No filter with connection catalog - lists columns in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of("system")),
                    ImmutableMultiset.of());

            // change the catalog back using a statement on the connection
            connection.createStatement().execute(String.format("USE %s.%s", COUNTING_CATALOG, "test_schema1"));
            // No filter with connection catalog - lists columns in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, null, null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    lists -> assertThat(lists.stream().map(list -> list.get(0)).collect(toImmutableSet()))
                            .isEqualTo(ImmutableSet.of(COUNTING_CATALOG)),
                    ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns"));

            // Equality predicate on schema name - lists columns from matching schemas in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, "test\\_schema1", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    IntStream.range(0, 1000).boxed()
                            .flatMap(tableIndex ->
                                    IntStream.range(0, 100)
                                            .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table" + tableIndex, "column_" + columnIndex, "varchar")))
                            .collect(toImmutableList()),
                    ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns(schema=test_schema1)"));

            // Equality predicate on schema name and table name - lists columns from matching schemas and tables in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, "test\\_schema1", "test\\_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    IntStream.range(0, 100)
                            .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + columnIndex, "varchar"))
                            .collect(toImmutableList()),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .build());

            // Equality predicate on schema name, table name and column name - lists matching columns in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, "test\\_schema1", "test\\_table1", "column\\_1"),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_1", "varchar")),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .build());

            // LIKE predicate on schema name, table name and column name - lists matching columns in the connection catalog
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(null, "test_schema1", "test_table1", "column_1"),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_1", "varchar")),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.listSchemaNames")
                            .add("ConnectorMetadata.listTables(schema=test_schema1)")
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .build());

            // LIKE predicate on schema name - lists columns from matching schemas in the connection catalog
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
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.listSchemaNames", 4)
                            .add("ConnectorMetadata.streamRelationColumns(schema=test_schema1)")
                            .build());

            // LIKE predicate on table name - lists columns from matching tables in the connection catalog
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
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.listSchemaNames", 5)
                            .add("ConnectorMetadata.listTables(schema=test_schema1)")
                            .add("ConnectorMetadata.listTables(schema=test_schema2)")
                            .add("ConnectorMetadata.listTables(schema=test_schema3_empty)")
                            .add("ConnectorMetadata.listTables(schema=test_schema4_empty)")
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 20)
                            .addCopies("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)", 5)
                            .addCopies("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)", 1)
                            .addCopies("ConnectorMetadata.isView(schema=test_schema1, table=test_table1)", 4)
                            .addCopies("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)", 5)
                            .addCopies("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)", 5)
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema2, table=test_table1)", 20)
                            .addCopies("ConnectorMetadata.getMaterializedView(schema=test_schema2, table=test_table1)", 5)
                            .addCopies("ConnectorMetadata.getView(schema=test_schema2, table=test_table1)", 1)
                            .addCopies("ConnectorMetadata.isView(schema=test_schema2, table=test_table1)", 4)
                            .addCopies("ConnectorMetadata.redirectTable(schema=test_schema2, table=test_table1)", 5)
                            .addCopies("ConnectorMetadata.getTableHandle(schema=test_schema2, table=test_table1)", 5)
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema2.test_table1)")
                            .build());
        }
    }

    private void testGetColumnsMetadataCalls(Connection connection)
            throws Exception
    {
        verify(connection.getMetaData().getSearchStringEscape().equals("\\")); // this test uses escape inline for readability

        // No filter
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns"));

        // Equality predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns"));

        // Equality predicate on catalog name and schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns(schema=test_schema1)"));

        // Equality predicate on catalog name, schema name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                IntStream.range(0, 100)
                        .mapToObj(i -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + i, "varchar"))
                        .collect(toImmutableList()),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // Equality predicate on catalog name, schema name, table name and column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test\\_schema1", "test\\_table1", "column\\_17"),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // Equality predicate on catalog name, LIKE predicate on schema name, table name and column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17"),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.listTables(schema=test_schema1)")
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // LIKE predicate on schema name and table name, but no predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                Stream.concat(
                        IntStream.range(0, 100)
                                .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + columnIndex, "varchar")),
                        IntStream.range(0, 2)
                                .mapToObj(columnIndex -> list("blackhole", "test_schema1", "test_table1", "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.listTables(schema=test_schema1)")
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // LIKE predicate on schema name, but no predicate on catalog name and table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test_schema1", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                Stream.concat(
                        IntStream.range(0, 1000).boxed()
                                .flatMap(tableIndex ->
                                        IntStream.range(0, 100)
                                                .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table" + tableIndex, "column_" + columnIndex, "varchar"))),
                        IntStream.range(0, 2)
                                .mapToObj(columnIndex -> list("blackhole", "test_schema1", "test_table1", "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.listSchemaNames", 4)
                        .add("ConnectorMetadata.streamRelationColumns(schema=test_schema1)")
                        .build());

        // LIKE predicate on table name, but no predicate on catalog name and schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, null, "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                Stream.concat(
                        IntStream.rangeClosed(1, 2).boxed()
                                .flatMap(schemaIndex ->
                                        IntStream.range(0, 100)
                                                .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema" + schemaIndex, "test_table1", "column_" + columnIndex, "varchar"))),
                        IntStream.range(0, 2)
                                .mapToObj(columnIndex -> list("blackhole", "test_schema1", "test_table1", "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.listSchemaNames", 5)
                        .add("ConnectorMetadata.listTables(schema=test_schema1)")
                        .add("ConnectorMetadata.listTables(schema=test_schema2)")
                        .add("ConnectorMetadata.listTables(schema=test_schema3_empty)")
                        .add("ConnectorMetadata.listTables(schema=test_schema4_empty)")
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 20)
                        .addCopies("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)", 5)
                        .addCopies("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)", 1)
                        .addCopies("ConnectorMetadata.isView(schema=test_schema1, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)", 5)
                        .addCopies("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)", 5)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema2, table=test_table1)", 20)
                        .addCopies("ConnectorMetadata.getMaterializedView(schema=test_schema2, table=test_table1)", 5)
                        .addCopies("ConnectorMetadata.getView(schema=test_schema2, table=test_table1)", 1)
                        .addCopies("ConnectorMetadata.isView(schema=test_schema2, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.redirectTable(schema=test_schema2, table=test_table1)", 5)
                        .addCopies("ConnectorMetadata.getTableHandle(schema=test_schema2, table=test_table1)", 5)
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema2.test_table1)")
                        .build());

        // Equality predicate on schema name and table name, but no predicate on catalog name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(null, "test\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                Stream.concat(
                        IntStream.range(0, 100)
                                .mapToObj(columnIndex -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + columnIndex, "varchar")),
                        IntStream.range(0, 2)
                                .mapToObj(columnIndex -> list("blackhole", "test_schema1", "test_table1", "column_" + columnIndex, "varchar")))
                        .collect(toImmutableList()),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // catalog does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns("wrong", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of());

        // empty catalog name (means null filter)
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns("", null, null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of());

        // schema does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "wrong\\_schema1", "test\\_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=wrong_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=wrong_schema1, table=test_table1)")
                        .build());

        // schema does not exist
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "wrong_schema1", "test_table1", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of("ConnectorMetadata.listSchemaNames"));

        // empty schema name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "", null, null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of());

        // empty table name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, "", null),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of());

        // empty column name
        assertMetadataCalls(
                connection,
                readMetaData(
                        databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, null, null, ""),
                        list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                list(),
                ImmutableMultiset.of("ConnectorMetadata.streamRelationColumns"));
    }

    @Test
    public void testAssumeLiteralMetadataCalls()
            throws Exception
    {
        testAssumeLiteralMetadataCalls("assumeLiteralNamesInMetadataCallsForNonConformingClients=true");
        testAssumeLiteralMetadataCalls("assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true");
        testAssumeLiteralMetadataCalls("assumeLiteralNamesInMetadataCallsForNonConformingClients=false&assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true");
        testAssumeLiteralMetadataCalls("assumeLiteralNamesInMetadataCallsForNonConformingClients=true&assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=false");
    }

    private void testAssumeLiteralMetadataCalls(String escapeLiteralParameter)
            throws Exception
    {
        try (Connection connection = DriverManager.getConnection(
                format("jdbc:trino://%s?%s", server.getAddress(), escapeLiteralParameter),
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
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                            .build());

            // getTables's schema and table name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "TABLE")),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.isView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .build());

            // no matches in getTables call as table name pattern treated as literal
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getTables(COUNTING_CATALOG, "test_schema_", null, null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE")),
                    list(),
                    ImmutableMultiset.<String>builder()
                            .add("ConnectorMetadata.getRelationTypes(schema=test_schema_)")
                            .build());

            // getColumns's schema and table name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    IntStream.range(0, 100)
                            .mapToObj(i -> list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_" + i, "varchar"))
                            .collect(toImmutableList()),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .build());

            // getColumns's schema, table and column name patterns treated as literals
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17"),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(list(COUNTING_CATALOG, "test_schema1", "test_table1", "column_17", "varchar")),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                            .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                            .build());

            // no matches in getColumns call as table name pattern treated as literal
            assertMetadataCalls(
                    connection,
                    readMetaData(
                            databaseMetaData -> databaseMetaData.getColumns(COUNTING_CATALOG, "test_schema1", "test_table_", null),
                            list("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME")),
                    list(),
                    ImmutableMultiset.<String>builder()
                            .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table_)", 4)
                            .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table_)")
                            .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table_)")
                            .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table_)")
                            .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table_)")
                            .build());
        }
    }

    @Test
    public void testFailedBothEscapeLiteralParameters()
    {
        assertThatThrownBy(() -> DriverManager.getConnection(
                format("jdbc:trino://%s?%s", server.getAddress(), "assumeLiteralNamesInMetadataCallsForNonConformingClients=true&assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true"),
                "admin",
                null))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Connection property assumeLiteralNamesInMetadataCallsForNonConformingClients cannot be set if assumeLiteralUnderscoreInMetadataCallsForNonConformingClients is enabled");
    }

    @Test
    public void testEscapeIfNecessary()
    {
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, null)).isEqualTo(null);
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, "a")).isEqualTo("a");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, "abc_def")).isEqualTo("abc_def");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, "abc__de_f")).isEqualTo("abc__de_f");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, "abc%def")).isEqualTo("abc%def");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, false, "abc\\_def")).isEqualTo("abc\\_def");

        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, null)).isEqualTo(null);
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, "a")).isEqualTo("a");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, "abc_def")).isEqualTo("abc\\_def");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, "abc__de_f")).isEqualTo("abc\\_\\_de\\_f");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, "abc%def")).isEqualTo("abc\\%def");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(true, false, "abc\\_def")).isEqualTo("abc\\\\\\_def");

        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, true, null)).isEqualTo(null);
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, true, "a")).isEqualTo("a");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, true, "abc_def")).isEqualTo("abc\\_def");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, true, "abc__de_f")).isEqualTo("abc\\_\\_de\\_f");
        assertThat(TrinoDatabaseMetaData.escapeIfNecessary(false, true, "abc\\_def")).isEqualTo("abc\\\\\\_def");
    }

    @Test
    public void testStatementsDoNotLeak()
            throws Exception
    {
        try (TrinoConnection connection = (TrinoConnection) createConnection()) {
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
    }

    private static void assertColumnSpec(ResultSet rs, int dataType, Long precision, Long numPrecRadix, String typeName)
            throws SQLException
    {
        String message = " of " + typeName + ": ";
        assertThat(rs.getObject("TYPE_NAME"))
                .describedAs("TYPE_NAME" + message)
                .isEqualTo(typeName);
        assertThat(rs.getObject("DATA_TYPE"))
                .describedAs("DATA_TYPE" + message)
                .isEqualTo((long) dataType);
        assertThat(rs.getObject("PRECISION"))
                .describedAs("PRECISION" + message)
                .isEqualTo(precision);
        assertThat(rs.getObject("LITERAL_PREFIX"))
                .describedAs("LITERAL_PREFIX" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("LITERAL_SUFFIX"))
                .describedAs("LITERAL_SUFFIX" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("CREATE_PARAMS"))
                .describedAs("CREATE_PARAMS" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("NULLABLE"))
                .describedAs("NULLABLE" + message)
                .isEqualTo((long) DatabaseMetaData.typeNullable);
        assertThat(rs.getObject("CASE_SENSITIVE"))
                .describedAs("CASE_SENSITIVE" + message)
                .isEqualTo(false);
        assertThat(rs.getObject("SEARCHABLE"))
                .describedAs("SEARCHABLE" + message)
                .isEqualTo((long) DatabaseMetaData.typeSearchable);
        assertThat(rs.getObject("UNSIGNED_ATTRIBUTE"))
                .describedAs("UNSIGNED_ATTRIBUTE" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("FIXED_PREC_SCALE"))
                .describedAs("FIXED_PREC_SCALE" + message)
                .isEqualTo(false);
        assertThat(rs.getObject("AUTO_INCREMENT"))
                .describedAs("AUTO_INCREMENT" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("LOCAL_TYPE_NAME"))
                .describedAs("LOCAL_TYPE_NAME" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("MINIMUM_SCALE"))
                .describedAs("MINIMUM_SCALE" + message)
                .isEqualTo(0L);
        assertThat(rs.getObject("MAXIMUM_SCALE"))
                .describedAs("MAXIMUM_SCALE" + message)
                .isEqualTo(0L);
        assertThat(rs.getObject("SQL_DATA_TYPE"))
                .describedAs("SQL_DATA_TYPE" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("SQL_DATETIME_SUB"))
                .describedAs("SQL_DATETIME_SUB" + message)
                .isEqualTo(null);
        assertThat(rs.getObject("NUM_PREC_RADIX"))
                .describedAs("NUM_PREC_RADIX" + message)
                .isEqualTo(numPrecRadix);
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

    private void assertMetadataCalls(Connection connection, MetaDataCallback<? extends Collection<List<Object>>> callback, Multiset<String> expectedMetadataCallsCount)
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
            Multiset<String> expectedMetadataCallsCount)
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
            Multiset<String> expectedMetadataCallsCount)
    {
        Multiset<String> actualMetadataCallsCount = countingMockConnector.runTracing(() -> {
            try {
                Collection<List<Object>> actual = callback.apply(connection.getMetaData());
                resultsVerification.accept(actual);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        actualMetadataCallsCount = actualMetadataCallsCount.stream()
                // Every query involves beginQuery and cleanupQuery, so ignore them.
                .filter(method -> !"ConnectorMetadata.beginQuery".equals(method) && !"ConnectorMetadata.cleanupQuery".equals(method))
                .collect(toImmutableMultiset());

        assertMultisetsEqual(actualMetadataCallsCount, expectedMetadataCallsCount);
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

    private Connection createConnectionWithNullCatalogMeansCurrent()
            throws SQLException
    {
        String url = format("jdbc:trino://%s?assumeNullCatalogMeansCurrentCatalog=true", server.getAddress());
        return DriverManager.getConnection(url, "admin", null);
    }

    private interface MetaDataCallback<T>
    {
        T apply(DatabaseMetaData metaData)
                throws SQLException;
    }
}
