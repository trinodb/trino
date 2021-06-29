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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryExecutor.QueryParam;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.utils.JdbcDriverUtils;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Named;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_MATCH;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TestHiveStorageFormats
        extends ProductTest
{
    private static final String TPCH_SCHEMA = "tiny";

    @Inject(optional = true)
    @Named("databases.presto.admin_role_enabled")
    private boolean adminRoleEnabled;

    private static final List<TimestampAndPrecision> TIMESTAMPS_FROM_HIVE = List.of(
            // write precision is not relevant here, as Hive always uses nanos
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111", // millis, no rounding
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111000",
                    "1967-01-02 12:01:00.111000000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1114", // hundreds of micros, rounds down in millis, (pre-epoch)
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111400",
                    "1967-01-02 12:01:00.111400000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1115", // hundreds of micros, rounds up in millis (smallest), pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.112",
                    "1967-01-02 12:01:00.111500",
                    "1967-01-02 12:01:00.111500000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111499", // micros, rounds down (largest), pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111499",
                    "1967-01-02 12:01:00.111499000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1113334", // hundreds of nanos, rounds down
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111333",
                    "1967-01-02 12:01:00.111333400"),
            timestampAndPrecision(
                    "1967-01-02 23:59:59.999999999", // nanos, rounds up to next day
                    NANOSECONDS,
                    "1967-01-03 00:00:00.000",
                    "1967-01-03 00:00:00.000000",
                    "1967-01-02 23:59:59.999999999"),

            timestampAndPrecision(
                    "1967-01-02 12:01:00.1110019", // hundreds of nanos, rounds down in millis and up in micros, pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111002",
                    "1967-01-02 12:01:00.111001900"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111901001", // nanos, rounds up in millis and down in micros, pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.112",
                    "1967-01-02 12:01:00.111901",
                    "1967-01-02 12:01:00.111901001"),
            timestampAndPrecision(
                    "1967-12-31 23:59:59.999999499", // nanos, rounds micros down (largest), rounds millis up to next year, pre-epoch
                    NANOSECONDS,
                    "1968-01-01 00:00:00.000",
                    "1967-12-31 23:59:59.999999",
                    "1967-12-31 23:59:59.999999499"),

            timestampAndPrecision(
                    "2027-01-02 12:01:00.1110019", // hundreds of nanos, rounds down in millis and up in micros, post-epoch
                    NANOSECONDS,
                    "2027-01-02 12:01:00.111",
                    "2027-01-02 12:01:00.111002",
                    "2027-01-02 12:01:00.111001900"),
            timestampAndPrecision(
                    "2027-01-02 12:01:00.111901001", // nanos, rounds up in millis and down in micros, post-epoch
                    NANOSECONDS,
                    "2027-01-02 12:01:00.112",
                    "2027-01-02 12:01:00.111901",
                    "2027-01-02 12:01:00.111901001"),
            timestampAndPrecision(
                    "2027-12-31 23:59:59.999999499", // nanos, rounds micros down (largest), rounds millis up to next year, post-epoch
                    NANOSECONDS,
                    "2028-01-01 00:00:00.000",
                    "2027-12-31 23:59:59.999999",
                    "2027-12-31 23:59:59.999999499"));

    // These check that values are correctly rounded on insertion
    private static final List<TimestampAndPrecision> TIMESTAMPS_FROM_TRINO = List.of(
            timestampAndPrecision(
                    "2020-01-02 12:01:00.999", // millis as millis (no rounding)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.999",
                    "2020-01-02 12:01:00.999000",
                    "2020-01-02 12:01:00.999000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111499999", // nanos as millis rounds down (largest)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111000",
                    "2020-01-02 12:01:00.111000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.1115", // micros as millis rounds up (smallest)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.112",
                    "2020-01-02 12:01:00.112000",
                    "2020-01-02 12:01:00.112000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333", // micros as micros (no rounding)
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.1113334", // nanos as micros rounds down
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333777", // nanos as micros rounds up
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111334",
                    "2020-01-02 12:01:00.111334000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333444", // nanos as nanos (no rounding)
                    NANOSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333444"),
            timestampAndPrecision(
                    "2020-01-02 23:59:59.999911333", // nanos as millis rounds up to next day
                    MILLISECONDS,
                    "2020-01-03 00:00:00.000",
                    "2020-01-03 00:00:00.000000",
                    "2020-01-03 00:00:00.000000000"),
            timestampAndPrecision(
                    "2020-12-31 23:59:59.99999", // micros as millis rounds up to next year
                    MILLISECONDS,
                    "2021-01-01 00:00:00.000",
                    "2021-01-01 00:00:00.000000",
                    "2021-01-01 00:00:00.000000000"));

    @DataProvider
    public static StorageFormat[] storageFormats()
    {
        return new StorageFormat[] {
                storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true")),
                storageFormat("PARQUET"),
                storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("SEQUENCEFILE"),
                storageFormat("TEXTFILE"),
                storageFormat("TEXTFILE", ImmutableMap.of(), ImmutableMap.of("textfile_field_separator", "F", "textfile_field_separator_escape", "E")),
                storageFormat("AVRO"),
        };
    }

    @DataProvider
    public static StorageFormat[] storageFormatsWithNullFormat()
    {
        return new StorageFormat[] {
                storageFormat("TEXTFILE"),
                storageFormat("RCTEXT"),
                storageFormat("SEQUENCEFILE")
        };
    }

    @DataProvider
    public static Iterator<StorageFormat> storageFormatsWithNanosecondPrecision()
    {
        return Stream.of(storageFormats())
                // everything but Avro supports nanoseconds
                .filter(format -> !"AVRO".equals(format.getName()))
                .iterator();
    }

    @Test(dataProvider = "storageFormats", groups = STORAGE_FORMATS)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
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

    @Test(dataProvider = "storageFormats", groups = STORAGE_FORMATS)
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

    @Test(dataProvider = "storageFormats", groups = STORAGE_FORMATS)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
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

    @Test(dataProvider = "storageFormatsWithNullFormat", groups = STORAGE_FORMATS)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testInsertAndSelectWithNullFormat(StorageFormat storageFormat)
    {
        String nullFormat = "null_value";
        String tableName = format(
                "test_storage_format_%s_insert_and_select_with_null_format",
                storageFormat.getName());
        query(format(
                "CREATE TABLE %s (value VARCHAR) " +
                        "WITH (format = '%s', null_format = '%s')",
                tableName,
                storageFormat.getName(),
                nullFormat));

        // \N is the default null format
        String[] values = new String[] {nullFormat, null, "non-null", "", "\\N"};
        Row[] storedValues = Arrays.stream(values).map(Row::row).toArray(Row[]::new);
        storedValues[0] = row((Object) null); // if you put in the null format, it saves as null

        String placeholders = String.join(", ", nCopies(values.length, "(?)"));
        query(
                format("INSERT INTO %s VALUES %s", tableName, placeholders),
                Arrays.stream(values)
                        .map(value -> param(JDBCType.VARCHAR, value))
                        .toArray(QueryParam[]::new));

        assertThat(query(format("SELECT * FROM %s", tableName))).containsOnly(storedValues);

        onHive().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storageFormatsWithNullFormat", groups = STORAGE_FORMATS)
    public void testSelectWithNullFormat(StorageFormat storageFormat)
    {
        String nullFormat = "null_value";
        String tableName = format(
                "test_storage_format_%s_select_with_null_format",
                storageFormat.getName());
        query(format(
                "CREATE TABLE %s (value VARCHAR) " +
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

    @Test(dataProvider = "storageFormats", groups = STORAGE_FORMATS)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
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
    public void testOrcTableCreatedInTrino()
    {
        onTrino().executeQuery("CREATE TABLE orc_table_created_in_trino WITH (format='ORC') AS SELECT 42 a");
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_trino"))
                .containsOnly(row(42));
        // Hive 3.1 validates (`org.apache.orc.impl.ReaderImpl#ensureOrcFooter`) ORC footer only when loading it from the cache, so when querying *second* time.
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_trino"))
                .containsOnly(row(42));
        assertThat(onHive().executeQuery("SELECT * FROM orc_table_created_in_trino WHERE a < 43"))
                .containsOnly(row(42));
        onTrino().executeQuery("DROP TABLE orc_table_created_in_trino");
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

    @Test
    public void testOrcStructsWithNonLowercaseFields()
            throws SQLException
    {
        String tableName = "orc_structs_with_non_lowercase";

        ensureDummyExists();
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onHive().executeQuery(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_struct struct<testCustId:string, requestDate:string>)" +
                        "STORED AS ORC ",
                tableName));

        onHive().executeQuery(format(
                "INSERT INTO %s"
                        // insert with SELECT because hive does not support array/map/struct functions in VALUES
                        + " SELECT"
                        + "   1,"
                        + "   named_struct('testCustId', '1234', 'requestDate', 'some day')"
                        // some hive versions don't allow INSERT from SELECT without FROM
                        + " FROM dummy",
                tableName));

        setSessionProperty(onTrino().getConnection(), "hive.projection_pushdown_enabled", "true");
        assertThat(onTrino().executeQuery("SELECT c_struct.testCustId FROM " + tableName)).containsOnly(row("1234"));
        assertThat(onTrino().executeQuery("SELECT c_struct.testcustid FROM " + tableName)).containsOnly(row("1234"));
        assertThat(onTrino().executeQuery("SELECT c_struct.requestDate FROM " + tableName)).containsOnly(row("some day"));
        setSessionProperty(onTrino().getConnection(), "hive.projection_pushdown_enabled", "false");
        assertThat(onTrino().executeQuery("SELECT c_struct.testCustId FROM " + tableName)).containsOnly(row("1234"));
        assertThat(onTrino().executeQuery("SELECT c_struct.testcustid FROM " + tableName)).containsOnly(row("1234"));
        assertThat(onTrino().executeQuery("SELECT c_struct.requestDate FROM " + tableName)).containsOnly(row("some day"));
    }

    @Test(dataProvider = "storageFormatsWithNanosecondPrecision")
    public void testTimestampCreatedFromHive(StorageFormat storageFormat)
    {
        String tableName = createSimpleTimestampTable("timestamps_from_hive", storageFormat);

        // insert records one by one so that we have one file per record, which
        // allows us to exercise predicate push-down in Parquet (which only
        // works when the value range has a min = max)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_HIVE) {
            onHive().executeQuery(format("INSERT INTO %s VALUES (%s, '%s')", tableName, entry.getId(), entry.getWriteValue()));
        }

        assertSimpleTimestamps(tableName, TIMESTAMPS_FROM_HIVE);
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "storageFormatsWithNanosecondPrecision")
    public void testTimestampCreatedFromTrino(StorageFormat storageFormat)
    {
        String tableName = createSimpleTimestampTable("timestamps_from_trino", storageFormat);

        // insert records one by one so that we have one file per record, which
        // allows us to exercise predicate push-down in Parquet (which only
        // works when the value range has a min = max)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_TRINO) {
            setTimestampPrecision(entry.getPrecision());
            onTrino().executeQuery(format("INSERT INTO %s VALUES (%s, TIMESTAMP '%s')", tableName, entry.getId(), entry.getWriteValue()));
        }

        assertSimpleTimestamps(tableName, TIMESTAMPS_FROM_TRINO);
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "storageFormatsWithNanosecondPrecision")
    public void testStructTimestampsFromHive(StorageFormat format)
    {
        String tableName = createStructTimestampTable("hive_struct_timestamp", format);
        setAdminRole(onTrino().getConnection());
        ensureDummyExists();

        // Insert one at a time because inserting with UNION ALL sometimes makes
        // data invisible to Trino (see https://github.com/trinodb/trino/issues/6485)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_HIVE) {
            onHive().executeQuery(format(
                    "INSERT INTO %1$s"
                            // insert with SELECT because hive does not support array/map/struct functions in VALUES
                            + " SELECT"
                            + "   %3$s,"
                            + "   array(%2$s),"
                            + "   map(%2$s, %2$s),"
                            + "   named_struct('col', %2$s),"
                            + "   array(map(%2$s, named_struct('col', array(%2$s))))"
                            // some hive versions don't allow INSERT from SELECT without FROM
                            + " FROM dummy",
                    tableName,
                    format("TIMESTAMP '%s'", entry.getWriteValue()),
                    entry.getId()));
        }

        assertStructTimestamps(tableName, TIMESTAMPS_FROM_HIVE);
        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storageFormatsWithNanosecondPrecision")
    public void testStructTimestampsFromTrino(StorageFormat format)
    {
        String tableName = createStructTimestampTable("trino_struct_timestamp", format);
        setAdminRole(onTrino().getConnection());

        // Insert data grouped by write-precision so it rounds as expected
        TIMESTAMPS_FROM_TRINO.stream()
                .collect(Collectors.groupingBy(TimestampAndPrecision::getPrecision))
                .forEach((precision, data) -> {
                    setTimestampPrecision(precision);
                    onTrino().executeQuery(format(
                            "INSERT INTO %s VALUES (%s)",
                            tableName,
                            data.stream().map(entry -> format(
                                    "%s,"
                                            + " array[%2$s],"
                                            + " map(array[%2$s], array[%2$s]),"
                                            + " row(%2$s),"
                                            + " array[map(array[%2$s], array[row(array[%2$s])])]",
                                    entry.getId(),
                                    format("TIMESTAMP '%s'", entry.getWriteValue())))
                                    .collect(Collectors.joining("), ("))));
                });

        assertStructTimestamps(tableName, TIMESTAMPS_FROM_TRINO);
        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    private String createSimpleTimestampTable(String tableNamePrefix, StorageFormat format)
    {
        return createTestTable(tableNamePrefix, format, "(id BIGINT, ts TIMESTAMP)");
    }

    /**
     * Assertions for tables created by {@link #createSimpleTimestampTable(String, StorageFormat)}
     */
    private static void assertSimpleTimestamps(String tableName, List<TimestampAndPrecision> data)
    {
        SoftAssertions softly = new SoftAssertions();
        for (TimestampAndPrecision entry : data) {
            for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
                setTimestampPrecision(precision);
                // Assert also with `CAST AS varchar` on the server side to avoid any JDBC-related issues
                softly.check(() -> assertThat(onTrino().executeQuery(
                        format("SELECT id, typeof(ts), CAST(ts AS varchar), ts FROM %s WHERE id = %s", tableName, entry.getId())))
                        .as("timestamp(%d)", precision.getPrecision())
                        .containsOnly(row(
                                entry.getId(),
                                entry.getReadType(precision),
                                entry.getReadValue(precision),
                                Timestamp.valueOf(entry.getReadValue(precision)))));
            }
        }
        softly.assertAll();
    }

    private String createStructTimestampTable(String tableNamePrefix, StorageFormat format)
    {
        return createTestTable(tableNamePrefix, format, ""
                + "("
                + "   id INTEGER,"
                + "   arr ARRAY(TIMESTAMP),"
                + "   map MAP(TIMESTAMP, TIMESTAMP),"
                + "   row ROW(col TIMESTAMP),"
                + "   nested ARRAY(MAP(TIMESTAMP, ROW(col ARRAY(TIMESTAMP))))"
                + ")");
    }

    /**
     * Assertions for tables created by {@link #createStructTimestampTable(String, StorageFormat)}
     */
    private void assertStructTimestamps(String tableName, Collection<TimestampAndPrecision> data)
    {
        SoftAssertions softly = new SoftAssertions();
        for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
            setTimestampPrecision(precision);

            // Check that the correct types are read
            String type = format("timestamp(%d)", precision.getPrecision());
            softly.check(() -> assertThat(onTrino()
                    .executeQuery(format(
                            "SELECT"
                                    + "   typeof(arr),"
                                    + "   typeof(map),"
                                    + "   typeof(row),"
                                    + "   typeof(nested)"
                                    + " FROM %s"
                                    + " LIMIT 1",
                            tableName)))
                    .as("timestamp container types")
                    .containsOnly(row(
                            format("array(%s)", type),
                            format("map(%1$s, %1$s)", type),
                            format("row(col %s)", type),
                            format("array(map(%1$s, row(col array(%1$s))))", type))));

            // Check the values as varchar
            softly.check(() -> assertThat(onTrino()
                    .executeQuery(format(
                            "SELECT"
                                    + "   id,"
                                    + "   CAST(arr[1] AS VARCHAR),"
                                    + "   CAST(map_entries(map)[1][1] AS VARCHAR)," // key
                                    + "   CAST(map_entries(map)[1][2] AS VARCHAR)," // value
                                    + "   CAST(row.col AS VARCHAR),"
                                    + "   CAST(map_entries(nested[1])[1][1] AS VARCHAR)," // key
                                    + "   CAST(map_entries(nested[1])[1][2].col[1] AS VARCHAR)" // value
                                    + " FROM %s"
                                    + " ORDER BY id",
                            tableName)))
                    .as("timestamp containers as varchar")
                    .containsExactlyInOrder(data.stream()
                            .sorted(comparingInt(TimestampAndPrecision::getId))
                            .map(e -> new Row(Lists.asList(
                                    e.getId(),
                                    nCopies(6, e.getReadValue(precision)).toArray())))
                            .collect(toList())));

            // Check the values directly
            softly.check(() -> assertThat(onTrino()
                    .executeQuery(format(
                            "SELECT"
                                    + "   id,"
                                    + "   arr[1],"
                                    + "   map_entries(map)[1][1]," // key
                                    + "   map_entries(map)[1][2]," // value
                                    + "   row.col,"
                                    + "   map_entries(nested[1])[1][1]," // key
                                    + "   map_entries(nested[1])[1][2].col[1]" // value
                                    + " FROM %s"
                                    + " ORDER BY id",
                            tableName)))
                    .as("timestamp containers")
                    .containsExactlyInOrder(data.stream()
                            .sorted(comparingInt(TimestampAndPrecision::getId))
                            .map(e -> new Row(Lists.asList(
                                    e.getId(),
                                    nCopies(6, Timestamp.valueOf(e.getReadValue(precision))).toArray())))
                            .collect(toList())));
        }
        softly.assertAll();
    }

    private String createTestTable(String tableNamePrefix, StorageFormat format, String sql)
    {
        // only admin user is allowed to change session properties
        setAdminRole(onTrino().getConnection());
        setSessionProperties(onTrino().getConnection(), format);

        String formatName = format.getName().toLowerCase(Locale.ENGLISH);
        String tableName = format("%s_%s_%s", tableNamePrefix, formatName, randomTableSuffix());
        onTrino().executeQuery(
                format("CREATE TABLE %s %s WITH (%s)", tableName, sql, format.getStoragePropertiesAsSql()));
        return tableName;
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

    /**
     * Ensures that a view named "dummy" with exactly one row exists in the default schema.
     */
    // These tests run on versions of Hive (1.1.0 on CDH 5) that don't fully support SELECT without FROM
    private void ensureDummyExists()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS dummy");
        onHive().executeQuery("CREATE TABLE dummy (dummy varchar(1))");
        onHive().executeQuery("INSERT INTO dummy VALUES ('x')");
    }

    /**
     * Set precision used when Trino reads and writes timestamps
     *
     * <p>(Hive always writes with nanosecond precision.)
     */
    private static void setTimestampPrecision(HiveTimestampPrecision readPrecision)
    {
        try {
            setSessionProperty(onTrino().getConnection(), "hive.timestamp_precision", readPrecision.name());
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

    /**
     * Create {@code TimestampAndPrecision} with specified write precision and rounded fractional seconds.
     *
     * @param writeValue The literal value to write.
     * @param precision Precision for writing value.
     * @param milliReadValue Expected value when reading with millisecond precision.
     * @param microReadValue Expected value when reading with microsecond precision.
     * @param nanoReadValue Expected value when reading with nanosecond precision.
     */
    private static TimestampAndPrecision timestampAndPrecision(
            String writeValue,
            HiveTimestampPrecision precision,
            String milliReadValue,
            String microReadValue,
            String nanoReadValue)
    {
        Map<HiveTimestampPrecision, String> readValues = Map.of(
                MILLISECONDS, milliReadValue,
                MICROSECONDS, microReadValue,
                NANOSECONDS, nanoReadValue);
        return new TimestampAndPrecision(precision, writeValue, readValues);
    }

    private static class TimestampAndPrecision
    {
        private static int counter;
        private final int id;
        // precision used when writing the data
        private final HiveTimestampPrecision precision;
        // inserted value
        private final String writeValue;
        // expected values to be read back at various precisions
        private final Map<HiveTimestampPrecision, String> readValues;

        public TimestampAndPrecision(HiveTimestampPrecision precision, String writeValue, Map<HiveTimestampPrecision, String> readValues)
        {
            this.id = counter++;
            this.precision = precision;
            this.writeValue = writeValue;
            this.readValues = readValues;
        }

        public int getId()
        {
            return id;
        }

        public HiveTimestampPrecision getPrecision()
        {
            return precision;
        }

        public String getWriteValue()
        {
            return writeValue;
        }

        public String getReadValue(HiveTimestampPrecision precision)
        {
            return requireNonNull(readValues.get(precision), () -> "no read value for " + precision);
        }

        public String getReadType(HiveTimestampPrecision precision)
        {
            return format("timestamp(%s)", precision.getPrecision());
        }
    }
}
