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
package io.prestosql.tests.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.assertions.QueryAssert.Row;
import io.prestosql.tempto.query.QueryExecutor.QueryParam;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.testng.services.Flaky;
import io.prestosql.tests.utils.JdbcDriverUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Named;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestosql.tempto.query.QueryExecutor.param;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.JdbcDriverUtils.setSessionProperty;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestHiveStorageFormats
        extends ProductTest
{
    private static final String TPCH_SCHEMA = "tiny";

    @Inject(optional = true)
    @Named("databases.presto.admin_role_enabled")
    private boolean adminRoleEnabled;

    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new StorageFormat[][] {
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true"))},
                {storageFormat("PARQUET")},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")},
                {storageFormat("TEXTFILE", ImmutableMap.of(), ImmutableMap.of("textfile_field_separator", "F", "textfile_field_separator_escape", "E"))},
                {storageFormat("AVRO")}
        };
    }

    @DataProvider(name = "storage_formats_with_null_format")
    public static Object[][] storageFormatsWithNullFormat()
    {
        return new StorageFormat[][] {
                {storageFormat("TEXTFILE")},
                {storageFormat("RCTEXT")},
                {storageFormat("SEQUENCEFILE")},
        };
    }

    @DataProvider
    public static Object[][] storageFormatsWithNanosecondPrecision()
    {
        return new StorageFormat[][] {
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true"))},
                {storageFormat("PARQUET")},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("RCTEXT")},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")},
                {storageFormat("TEXTFILE", ImmutableMap.of(), ImmutableMap.of("textfile_field_separator", "F", "textfile_field_separator_escape", "E"))}
        };
    }

    @Test(dataProvider = "storage_formats", groups = STORAGE_FORMATS)
    public void testInsertIntoTable(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setAdminRole();
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (%s)",
                tableName,
                storageFormat.getStoragePropertiesAsSql());
        query(createTable);

        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        assertResultEqualForLineitemTable(
                "select sum(tax), sum(discount), sum(linenumber) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = STORAGE_FORMATS)
    public void testCreateTableAs(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setAdminRole();
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (%s) AS " +
                        "SELECT " +
                        "partkey, suppkey, extendedprice " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getStoragePropertiesAsSql(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertResultEqualForLineitemTable(
                "select sum(extendedprice), sum(suppkey), count(partkey) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = STORAGE_FORMATS)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/2390", match = "File .* could only be written to 0 of the 1 minReplication nodes")
    public void testInsertIntoPartitionedTable(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setAdminRole();
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_partitioned_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s', partitioned_by = ARRAY['returnflag'])",
                tableName,
                storageFormat.getName());
        query(createTable);

        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        assertResultEqualForLineitemTable(
                "select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats_with_null_format", groups = STORAGE_FORMATS)
    public void testInsertAndSelectWithNullFormat(StorageFormat storageFormat)
    {
        String nullFormat = "null_value";
        String tableName = format("test_storage_format_%s_insert_and_select_with_null_format",
                storageFormat.getName());
        query(format("CREATE TABLE %s (value VARCHAR) " +
                        "WITH (format = '%s', null_format = '%s')",
                tableName,
                storageFormat.getName(),
                nullFormat));

        // \N is the default null format
        String[] values = new String[] {nullFormat, null, "non-null", "", "\\N"};
        Row[] storedValues = Arrays.stream(values).map(Row::row).toArray(Row[]::new);
        storedValues[0] = row((Object) null); // if you put in the null format, it saves as null

        String placeholders = String.join(", ", Collections.nCopies(values.length, "(?)"));
        query(format("INSERT INTO %s VALUES %s", tableName, placeholders),
                Arrays.stream(values)
                        .map(value -> param(JDBCType.VARCHAR, value))
                        .toArray(QueryParam[]::new));

        assertThat(query(format("SELECT * FROM %s", tableName))).containsOnly(storedValues);

        onHive().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats_with_null_format", groups = STORAGE_FORMATS)
    public void testSelectWithNullFormat(StorageFormat storageFormat)
    {
        String nullFormat = "null_value";
        String tableName = format("test_storage_format_%s_select_with_null_format",
                storageFormat.getName());
        query(format("CREATE TABLE %s (value VARCHAR) " +
                        "WITH (format = '%s', null_format = '%s')",
                tableName,
                storageFormat.getName(),
                nullFormat));

        // Manually format data for insertion b/c Hive's PreparedStatement can't handle nulls
        onHive().executeQuery(format("INSERT INTO %s VALUES ('non-null'), (NULL), ('%s')",
                tableName, nullFormat));

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .containsOnly(row("non-null"), row((Object) null), row((Object) null));

        onHive().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = STORAGE_FORMATS)
    public void testCreatePartitionedTableAs(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setAdminRole();
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_partitioned_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (%s, partitioned_by = ARRAY['returnflag']) AS " +
                        "SELECT " +
                        "tax, discount, returnflag " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getStoragePropertiesAsSql(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertResultEqualForLineitemTable(
                "select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS)
    public void testOrcTableCreatedInPresto()
    {
        onPresto().executeQuery("CREATE TABLE orc_table_created_in_presto WITH (format='ORC') AS SELECT 42 a");
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_presto"))
                .containsOnly(row(42));
        // Hive 3.1 validates (`org.apache.orc.impl.ReaderImpl#ensureOrcFooter`) ORC footer only when loading it from the cache, so when querying *second* time.
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_presto"))
                .containsOnly(row(42));
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_presto WHERE a < 43"))
                .containsOnly(row(42));
        onPresto().executeQuery("DROP TABLE orc_table_created_in_presto");
    }

    @Test(groups = STORAGE_FORMATS)
    public void testSnappyCompressedParquetTableCreatedInHive()
    {
        String tableName = "table_created_in_hive_parquet";

        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onHive().executeQuery(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_varchar VARCHAR(255))" +
                        "STORED AS PARQUET " +
                        "TBLPROPERTIES(\"parquet.compression\"=\"SNAPPY\")",
                tableName));

        onHive().executeQuery(format("INSERT INTO %s VALUES(1, 'test data')", tableName));

        assertThat(query("SELECT * FROM " + tableName)).containsExactly(row(1, "test data"));

        onHive().executeQuery("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "storageFormatsWithNanosecondPrecision", groups = STORAGE_FORMATS)
    public void testTimestamp(StorageFormat storageFormat)
            throws Exception
    {
        // only admin user is allowed to change session properties
        Connection connection = onPresto().getConnection();
        setAdminRole(connection);
        setSessionProperties(connection, storageFormat);

        String tableName = "test_timestamp_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);
        onPresto().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onPresto().executeQuery(format("CREATE TABLE %s (id BIGINT, ts TIMESTAMP) WITH (%s)", tableName, storageFormat.getStoragePropertiesAsSql()));
        List<TimestampAndPrecision> data = ImmutableList.of(
                new TimestampAndPrecision(1, "MILLISECONDS", "2020-01-02 12:34:56.123", "2020-01-02 12:34:56.123"),
                new TimestampAndPrecision(2, "MILLISECONDS", "2020-01-02 12:34:56.1234", "2020-01-02 12:34:56.123"),
                new TimestampAndPrecision(3, "MILLISECONDS", "2020-01-02 12:34:56.1236", "2020-01-02 12:34:56.124"),
                new TimestampAndPrecision(4, "MICROSECONDS", "2020-01-02 12:34:56.123456", "2020-01-02 12:34:56.123456"),
                new TimestampAndPrecision(5, "MICROSECONDS", "2020-01-02 12:34:56.1234564", "2020-01-02 12:34:56.123456"),
                new TimestampAndPrecision(6, "MICROSECONDS", "2020-01-02 12:34:56.1234567", "2020-01-02 12:34:56.123457"),
                new TimestampAndPrecision(7, "NANOSECONDS", "2020-01-02 12:34:56.123456789", "2020-01-02 12:34:56.123456789"));

        // insert records one by one so that we have one file per record, which allows us to exercise predicate push-down in Parquet
        // (which only works when the value range has a min = max)
        for (TimestampAndPrecision entry : data) {
            onHive().executeQuery(format("INSERT INTO %s VALUES (%s, '%s')", tableName, entry.getId(), entry.getValue()));
        }

        for (TimestampAndPrecision entry : data) {
            setSessionProperty(connection, "hive.timestamp_precision", entry.getPrecision());
            assertThat(onPresto().executeQuery(format("SELECT ts FROM %s WHERE id = %s", tableName, entry.getId())))
                    .containsOnly(row(Timestamp.valueOf(entry.getRoundedValue())));
            assertThat(onPresto().executeQuery(format("SELECT id FROM %s WHERE id = %s AND ts = TIMESTAMP'%s'", tableName, entry.getId(), entry.getRoundedValue())))
                    .containsOnly(row(entry.getId()));
            if (entry.isRoundedUp()) {
                assertThat(onPresto().executeQuery(format("SELECT id FROM %s WHERE id = %s AND ts > TIMESTAMP'%s'", tableName, entry.getId(), entry.getValue())))
                        .containsOnly(row(entry.getId()));
            }
            if (entry.isRoundedDown()) {
                assertThat(onPresto().executeQuery(format("SELECT id FROM %s WHERE id = %s AND ts < TIMESTAMP'%s'", tableName, entry.getId(), entry.getValue())))
                        .containsOnly(row(entry.getId()));
            }
        }
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    /**
     * Run the given query on the given table and the TPCH {@code lineitem} table
     * (in the schema {@code TPCH_SCHEMA}, asserting that the results are equal.
     */
    private static void assertResultEqualForLineitemTable(String query, String tableName)
    {
        QueryResult expected = query(format(query, "tpch." + TPCH_SCHEMA + ".lineitem"));
        List<Row> expectedRows = expected.rows().stream()
                .map((columns) -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = query(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsExactly(expectedRows);
    }

    private void setAdminRole()
    {
        setAdminRole(defaultQueryExecutor().getConnection());
    }

    private void setAdminRole(Connection connection)
    {
        if (adminRoleEnabled) {
            return;
        }

        try {
            JdbcDriverUtils.setRole(connection, "admin");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setSessionProperties(StorageFormat storageFormat)
    {
        setSessionProperties(defaultQueryExecutor().getConnection(), storageFormat);
    }

    private static void setSessionProperties(Connection connection, StorageFormat storageFormat)
    {
        setSessionProperties(connection, storageFormat.getSessionProperties());
    }

    private static void setSessionProperties(Connection connection, Map<String, String> sessionProperties)
    {
        try {
            // create more than one split
            setSessionProperty(connection, "task_writer_count", "4");
            setSessionProperty(connection, "redistribute_writes", "false");
            for (Map.Entry<String, String> sessionProperty : sessionProperties.entrySet()) {
                setSessionProperty(connection, sessionProperty.getKey(), sessionProperty.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(
            String name,
            Map<String, String> sessionProperties,
            Map<String, String> properties)
    {
        return new StorageFormat(name, sessionProperties, properties);
    }

    private static class StorageFormat
    {
        private final String name;
        private final Map<String, String> properties;
        private final Map<String, String> sessionProperties;

        private StorageFormat(
                String name,
                Map<String, String> sessionProperties,
                Map<String, String> properties)
        {
            this.name = requireNonNull(name, "name is null");
            this.properties = requireNonNull(properties, "properties is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public String getStoragePropertiesAsSql()
        {
            return Stream.concat(
                    Stream.of(immutableEntry("format", name)),
                    properties.entrySet().stream())
                    .map(entry -> format("%s = '%s'", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(", "));
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("properties", properties)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }

    private static class TimestampAndPrecision
    {
        private final int id;
        private final String precision;
        private final String value;
        private final String roundedValue;

        public TimestampAndPrecision(int id, String precision, String value, String roundedValue)
        {
            this.id = id;
            this.precision = precision;
            this.value = value;
            this.roundedValue = roundedValue;
        }

        public int getId()
        {
            return id;
        }

        public String getPrecision()
        {
            return precision;
        }

        public String getValue()
        {
            return value;
        }

        public String getRoundedValue()
        {
            return roundedValue;
        }

        private int roundingSign()
        {
            return Timestamp.valueOf(roundedValue).compareTo(Timestamp.valueOf(value));
        }

        public boolean isRoundedUp()
        {
            return roundingSign() > 0;
        }

        public boolean isRoundedDown()
        {
            return roundingSign() < 0;
        }
    }
}
